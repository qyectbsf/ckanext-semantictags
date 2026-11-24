import logging
import os
import json
import re

import ckan.lib.base as base
from ckan.lib.redis import connect_to_redis
import ckan.logic as logic
import ckan.model as model
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import requests as http_requests
from ckan.common import config
from ckan.logic.action.create import package_create as core_package_create
from ckan.logic.action.get import package_show as core_package_show
from ckan.logic.action.update import package_update as core_package_update
from ckan.plugins.toolkit import asbool
from flask import Blueprint, request, jsonify

from ckanext.semantictags import cli

from ckanext.semantictags.helpers import (
    ONTOLOGIES_KEY, FREE_TAGS_KEY, FORCE_RELOAD_KEY, UPDATE_FREQUENCY_KEY, TAG_SUGGEST_KEY,
    generate_tag_vocabulary, LDM_tags_util, resolve_vocab_tags,
    search_ontologies,
    get_available_ontologies,
)
from ckanext.semantictags.tasks import Scheduler
from ckanext.semantictags.model import tag as tag_model
from ckanext.semantictags.model import umls_cache as umls_cache_model
from ckanext.semantictags.model.crud import OntologyManager, TagQuery, VocabularyQuery
from ckanext.semantictags.model.umls_cache import UMLSCacheQuery

NotFound = logic.NotFound

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

TIB_TS_API = 'https://api.terminology.tib.eu/api/v2'

UMLS_API_BASE = 'https://uts-ws.nlm.nih.gov/rest'
UMLS_IRI_PREFIX = 'http://linkedlifedata.com/resource/umls/id/'

_INVALID_TAG_CHARS = re.compile(r'[^\w\s.\-]', re.UNICODE)
_PENDING_UMLS_TTL = 3600  # 1 hour


def _sanitize_tag_name(name, max_len=100):
    clean = _INVALID_TAG_CHARS.sub('', name or '')
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean[:max_len].strip()


def _pending_key(tag_name):
    return f'semantictags:umls:pending:{tag_name.strip().lower()}'


def _stash_pending_umls(tag_name, payload):
    try:
        redis_conn = connect_to_redis()
        redis_conn.set(_pending_key(tag_name), json.dumps(payload), ex=_PENDING_UMLS_TTL)
    except Exception as e:
        log.warning(f'Could not cache pending UMLS tag "{tag_name}" in Redis: {e}')


def _pop_pending_umls(tag_name):
    try:
        redis_conn = connect_to_redis()
        raw = redis_conn.get(_pending_key(tag_name))
        if not raw:
            return None
        return json.loads(raw.decode('utf-8') if isinstance(raw, bytes) else raw)
    except Exception as e:
        log.warning(f'Could not read pending UMLS tag "{tag_name}" from Redis: {e}')
        return None

def _extract_submitted_tag_names(data_dict):
    names = []
    tag_string = data_dict.get('tag_string')
    if isinstance(tag_string, str) and tag_string.strip():
        names.extend(t.strip() for t in tag_string.split(',') if t.strip())
    for tag in data_dict.get('tags') or []:
        if isinstance(tag, dict) and tag.get('name'):
            names.append(tag['name'].strip())
        elif isinstance(tag, str) and tag.strip():
            names.append(tag.strip())
    return names


def _persist_selected_umls_tags(data_dict):
    for name in _extract_submitted_tag_names(data_dict):
        if UMLSCacheQuery.get_by_tag_name(name):
            continue
        pending = _pop_pending_umls(name)
        if not pending:
            continue
        UMLSCacheQuery.upsert(
            cui=pending['cui'],
            ckan_tag_name=pending['ckan_tag_name'],
            preferred_name=pending['preferred_name'],
            iri=pending['iri'],
        )

def _asbool_safe(value, default=False):
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    return asbool(value)

def _get_umls_api_key():
    return (
        config.get('ckan.umls.api_key')
        or config.get('umls.api_key')
        or os.environ.get('CKAN__UMLS__API_KEY')
    )


def _search_umls(query, limit=10):
    query = (query or '').strip()
    if len(query) < 3:
        return []

    results = []
    seen_cuis = set()

    # 1. Local cache of already-saved UMLS tags
    for cached in UMLSCacheQuery.search(query, limit=limit):
        seen_cuis.add(cached.cui)
        results.append({
            'id': cached.ckan_tag_name,
            'text': f'{cached.preferred_name} ({cached.cui})',
            'ontology': 'umls',
        })

    # 2. Live UMLS API for new terms
    api_key = _get_umls_api_key()
    if not api_key or len(results) >= limit:
        return results[:limit]

    try:
        resp = http_requests.get(
            f'{UMLS_API_BASE}/search/current',
            params={'string': query, 'apiKey': api_key, 'pageSize': limit},
            timeout=5,
        )
        resp.raise_for_status()
        items = resp.json().get('result', {}).get('results', [])
    except http_requests.RequestException as e:
        log.warning(f'UMLS search failed for "{query}": {e}')
        return results

    for item in items:
        cui = item.get('ui', '')
        name = item.get('name', '')
        if not cui or cui == 'NONE' or not name or cui in seen_cuis:
            continue

        clean_name = _sanitize_tag_name(name)
        if len(clean_name) < 2:
            continue

        seen_cuis.add(cui)
        iri = f'{UMLS_IRI_PREFIX}{cui}'
        _stash_pending_umls(clean_name, {
            'cui': cui,
            'ckan_tag_name': clean_name,
            'preferred_name': name,
            'iri': iri,
        })

        results.append({
            'id': clean_name,
            'text': f'{name} ({cui})',
            'ontology': 'umls',
        })
        if len(results) >= limit:
            break

    return results

@toolkit.side_effect_free
def term_details(context, data_dict):
    """
    Fetch term details by IRI from the local UMLS cache / UMLS UTS API
    (for UMLS IRIs) or TIB Terminology Service (OLS4 v2).
    """
    iri = data_dict.get('iri', '').strip()
    if not iri:
        raise toolkit.ValidationError({'iri': ['Missing value']})

    if iri.startswith(UMLS_IRI_PREFIX):
        cui = iri[len(UMLS_IRI_PREFIX):].strip('/')
        cached = UMLSCacheQuery.get_by_iri(iri) or UMLSCacheQuery.get_by_cui(cui)

        # 1. Full cache hit (definition already fetched, including "" for none)
        if cached and cached.definition is not None:
            return {
                'label': cached.preferred_name,
                'iri': cached.iri,
                'curie': f'UMLS:{cached.cui}',
                'ontology_name': 'umls',
                'definition': cached.definition or None,
            }

        api_key = _get_umls_api_key()
        if not api_key:
            # If we at least have the cached label, return it without a definition
            if cached:
                return {
                    'label': cached.preferred_name,
                    'iri': cached.iri,
                    'curie': f'UMLS:{cached.cui}',
                    'ontology_name': 'umls',
                    'definition': None,
                }
            return {'error': 'UMLS API key not configured'}

        try:
            # 2. Label: reuse cached preferred_name on partial hit, else fetch from UMLS
            if cached:
                label = cached.preferred_name
            else:
                cui_resp = http_requests.get(
                    f'{UMLS_API_BASE}/content/current/CUI/{cui}',
                    params={'apiKey': api_key},
                    timeout=5,
                )
                cui_resp.raise_for_status()
                concept = cui_resp.json().get('result', {})
                label = concept.get('name') or cui

            # 3. Definition: fetch once from UMLS
            definition = ''
            try:
                def_resp = http_requests.get(
                    f'{UMLS_API_BASE}/content/current/CUI/{cui}/definitions',
                    params={'apiKey': api_key, 'pageSize': 1},
                    timeout=5,
                )
                if def_resp.ok:
                    defs = def_resp.json().get('result', [])
                    if isinstance(defs, list) and defs:
                        definition = defs[0].get('value') or ''
                elif def_resp.status_code != 404:
                    # Transient error (e.g., 500/429): keep None so we retry next time
                    definition = None
            except http_requests.RequestException as e:
                log.debug(f'UMLS definitions fetch failed for {cui}: {e}')
                definition = None

            # 4. Write-back definition to PostgreSQL for cached tags
            if cached and definition is not None:
                UMLSCacheQuery.upsert(
                    cui=cached.cui,
                    ckan_tag_name=cached.ckan_tag_name,
                    preferred_name=cached.preferred_name,
                    iri=cached.iri,
                    definition=definition,
                )

            return {
                'label': label,
                'iri': iri,
                'curie': f'UMLS:{cui}',
                'ontology_name': 'umls',
                'definition': definition or None,
            }
        except http_requests.RequestException as e:
            log.warning(f'UMLS term_details failed for {cui}: {e}')
            if cached:
                return {
                    'label': cached.preferred_name,
                    'iri': cached.iri,
                    'curie': f'UMLS:{cached.cui}',
                    'ontology_name': 'umls',
                    'definition': None,
                }
            return {'error': 'UMLS service unavailable'}

    # Non-UMLS IRIs continue to use TIB Terminology Service
    try:
        term_resp = http_requests.get(
            f'{TIB_TS_API}/entities',
            params={'iri': iri, 'size': 1},
            timeout=5,
        )
        term_resp.raise_for_status()
        term_data = term_resp.json()

        elements = term_data.get('elements', [])
        if not elements:
            return {'error': 'Term not found'}

        term = elements[0]
        ontology_id = term.get('ontologyId', '')

        description = term.get('definition') or []
        definition = description[0] if description else None

        return {
            'label': term.get('label'),
            'iri': term.get('iri'),
            'curie': term.get('curie'),
            'ontology_name': ontology_id,
            'definition': definition,
        }

    except http_requests.RequestException as e:
        log.warning(f'TIB TS request failed for IRI {iri}: {e}')
        return {'error': 'Terminology service unavailable'}


@toolkit.side_effect_free
def autocomplete_term(context, data_dict):
    """
    Autocomplete tags from ontologies.

    :param q: partial query string (preferred)
    :type q: str
    :param incomplete: partial query string (fallback)
    :type incomplete: str
    :param limit: maximum number of results (default 10)
    :type limit: int

    :returns: list of tag labels/names, e.g. ["Tag 1", "Tag 2", "..."]
    """
    query = data_dict.get('q') or data_dict.get('incomplete', '')
    limit = int(data_dict.get('limit') or 10)

    results = []

    tags_util = LDM_tags_util()
    vocab = VocabularyQuery.read_name(tags_util.vocabulary_name_default)

    if vocab:
        res = OntologyManager.search_terms(query, limit=limit, vocabulary_id=vocab.id)
        for t in res:
            label = getattr(t, 'label', None) or t.name
            ontology = getattr(t, 'ontology', '') or ''
            results.append({'id': label, 'text': label, 'ontology': ontology})

    results.extend(_search_umls(query, limit=limit))
    return results

def suggest_tags_from_text(context, data_dict):
    """
    Suggest tags from description text using NFDI4Energy Annotator API.
    """
    text = data_dict.get('text', '').strip()
    if not text:
        raise toolkit.ValidationError({'text': ['Missing or empty text']})

    # determine which ontologies to query (either passed or configured)
    tags_util = LDM_tags_util()
    ontology_ids = data_dict.get('ontology_ids') or tags_util.ontologies
    if not ontology_ids:
        log.error('No ontologies configured')
        return {'suggestions': [], 'count': 0, 'error': 'No ontologies configured'}

    try:
        log.info('Submitting request')
        annotator_response = http_requests.post(
            'https://service.tib.eu/sandbox/nfdi4energyannotator/annotate',
            json={
                'text': text,
                'ontology_ids': ontology_ids,
                'max_depth': 0
            },
            timeout=30
        )
        log.info('Request answered')
        annotator_response.raise_for_status()
        data = annotator_response.json()

        matches = data.get('matches', [])

        # only keep matches from the requested ontologies and dedupe by iri
        ontology_set = set(ontology_ids)
        seen_iris = {}
        for match in matches:
            iri = match.get('iri')
            ontology = (match.get('ontologyId') or '').lower()
            if iri and ontology in ontology_set:
                if iri not in seen_iris or match.get('score', 0) > seen_iris[iri]['score']:
                    seen_iris[iri] = match

        # build the result list using same field names as term_details
        suggested_tags = []
        for match in seen_iris.values():
            suggested_tags.append({
                'label': match.get('label', match.get('token', '')),
                'iri': match.get('iri'),
                'curie': match.get('curie'),
                'ontology_name': match.get('ontologyId'),
            })

        return {'suggestions': suggested_tags, 'count': len(suggested_tags)}

    except http_requests.RequestException as e:
        log.error(f'Annotator request failed: {e}')
        return {'error': 'Annotation service unavailable', 'suggestions': []}

@toolkit.side_effect_free
def ontology_search_action(context, data_dict):
    query = data_dict.get('q', '')
    limit = int(data_dict.get('limit', 20))
    return search_ontologies(query, limit=limit)


@toolkit.side_effect_free
def package_show(context, data_dict):
    data = core_package_show(context, data_dict)
    tags = data.get('tags', [])
    if not tags:
        return data

    for tag in tags:
        tag_id = tag.get('id')
        tag_row = TagQuery.read(tag_id)
        if tag_row:
            tag['iri'] = tag_row.iri
            tag['ontology'] = tag_row.ontology
            if hasattr(tag_row, 'label') and tag_row.label:
                tag['display_name'] = tag_row.label
            if getattr(tag_row, 'vocabulary_id', None) and not tag.get('vocabulary_id'):
                tag['vocabulary_id'] = tag_row.vocabulary_id
            if tag_row.iri and tag_row.iri.startswith(UMLS_IRI_PREFIX):
                tag['umls_cui'] = tag_row.iri[len(UMLS_IRI_PREFIX):].strip('/')
            continue

        umls_row = UMLSCacheQuery.get_by_tag_name(tag.get('name'))
        if umls_row:
            tag['iri'] = umls_row.iri
            tag['ontology'] = 'umls'
            tag['umls_cui'] = umls_row.cui
            tag['display_name'] = umls_row.preferred_name

    return data

def package_create(context, data_dict):
    _persist_selected_umls_tags(data_dict)
    resolve_vocab_tags(data_dict)
    return core_package_create(context, data_dict)


def package_update(context, data_dict):
    _persist_selected_umls_tags(data_dict)
    resolve_vocab_tags(data_dict)
    return core_package_update(context, data_dict)


def get_data_module_source():
    return '/api/3/action/semantictags_autocomplete?incomplete=?'


def get_loaded_ontologies():
    return OntologyManager.list_ontologies()

def free_tags_allowed():
    return _asbool_safe(config.get(FREE_TAGS_KEY), default=False)

def tag_suggestions_enabled():
    return _asbool_safe(config.get(TAG_SUGGEST_KEY), default=False)

def _check_access():
    context = {
        'model': model,
        'session': model.Session,
        'user': toolkit.c.user,
        'auth_user_obj': toolkit.c.userobj
    }
    try:
        logic.check_access('sysadmin', context, {})
    except logic.NotAuthorized:
        base.abort(403, toolkit._('Need to be system administrator to administer.'))


class LDMtagsPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IClick)
    plugins.implements(plugins.IPackageController, inherit=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        tag_model.init_table()
        umls_cache_model.init_table()
        self.scheduler = None

    def before_create(self, context, data_dict):
        _persist_selected_umls_tags(data_dict)
        resolve_vocab_tags(data_dict)
        return data_dict

    def before_update(self, context, data_dict):
        _persist_selected_umls_tags(data_dict)
        resolve_vocab_tags(data_dict)
        return data_dict

    def get_commands(self):
        return cli.get_commands()

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_resource('static', 'semantictags')
        toolkit.add_ckan_admin_tab(config_, 'semantictags.admin', 'SemanticTags', icon='tags')

        if config_.get(UPDATE_FREQUENCY_KEY, None) is None:
            config_[UPDATE_FREQUENCY_KEY] = 60

        for key in (FREE_TAGS_KEY, FORCE_RELOAD_KEY, TAG_SUGGEST_KEY):
            if not config_.get(key):
                config_[key] = 'false'

        self.scheduler = Scheduler()

    def update_config_schema(self, schema):
        ignore_missing = toolkit.get_validator('ignore_missing')
        int_validator = toolkit.get_validator('int_validator')

        schema.update({
            ONTOLOGIES_KEY: [ignore_missing],
            FREE_TAGS_KEY: [ignore_missing],
            FORCE_RELOAD_KEY: [ignore_missing],
            TAG_SUGGEST_KEY: [ignore_missing],
            UPDATE_FREQUENCY_KEY: [ignore_missing, int_validator],
        })

        return schema

    def get_helpers(self):
        """Register SemanticTags template helpers."""
        # Template helper function names should begin with the name of the
        # extension they belong to, to avoid clashing with functions from
        # other extensions.
        return {
            'semantictags_data_module_source': get_data_module_source,
            'semantictags_available_ontologies': get_loaded_ontologies,
            'semantictags_enable_freetags': free_tags_allowed,
            'semantictags_enable_suggestions': tag_suggestions_enabled
        }

    def get_actions(self):
        return {
            'semantictags_autocomplete': autocomplete_term,
            'semantictags_term_details': term_details,
            'semantictags_suggest_tags': suggest_tags_from_text,
            'semantictags_ontology_search': ontology_search_action,
            'package_show': package_show,
            'package_create': package_create,
            'package_update': package_update
        }

    def get_blueprint(self):
        blueprint = Blueprint('semantictags', __name__)

        @blueprint.route('/ckan-admin/semantictags', methods=['GET', 'POST'])
        def admin():
            _check_access()

            if request.method == 'POST':
                action = request.form.get('action', None)

                if action == 'ontologies':
                    ontologies_str = request.form.get(ONTOLOGIES_KEY, '').strip()
                    ontologies_list = [o.strip() for o in ontologies_str.split(',') if o.strip()]

                    if not ontologies_list:
                        toolkit.h.flash_error(toolkit._('Please select at least one ontology.'))
                    else:
                        logic.get_action(u'config_option_update')({
                            u'user': toolkit.c.user
                        }, {
                            ONTOLOGIES_KEY: ' '.join(ontologies_list)
                        })
                        generate_tag_vocabulary(ontologies=ontologies_list)
                        toolkit.h.flash_success(toolkit._('New ontologies set successfully.'))

                elif action == 'free_tags':
                    free_tags = request.form.get(FREE_TAGS_KEY)
                    try:
                        free_tags = asbool(free_tags)
                    except (ValueError, TypeError):
                        toolkit.h.flash_error(toolkit._('Please specify a value that can be parsed as a boolean.'))
                    logic.get_action(u'config_option_update')({
                        u'user': toolkit.c.user
                    }, {
                        FREE_TAGS_KEY: 'true' if free_tags else 'false'
                    })
                    toolkit.h.flash_success(toolkit._('Free tags option updated successfully.'))
                elif action == 'force_reload':
                    force_reload = request.form.get(FORCE_RELOAD_KEY)
                    try:
                        force_reload = asbool(force_reload)
                    except (ValueError, TypeError):
                        toolkit.h.flash_error(toolkit._('Please specify a value that can be parsed as a boolean.'))
                    logic.get_action(u'config_option_update')({
                        u'user': toolkit.c.user
                    }, {
                        FORCE_RELOAD_KEY: 'true' if force_reload else 'false'
                    })
                    toolkit.h.flash_success(toolkit._('Force reload option updated successfully.'))
                elif action == 'tag_suggest':
                    tag_suggest = request.form.get(TAG_SUGGEST_KEY)
                    try:
                        tag_suggest = asbool(tag_suggest)
                    except (ValueError, TypeError):
                        toolkit.h.flash_error(toolkit._('Please specify a value that can be parsed as a boolean.'))
                    logic.get_action(u'config_option_update')({
                        u'user': toolkit.c.user
                    }, {
                        TAG_SUGGEST_KEY: 'true' if tag_suggest else 'false'
                    })
                    toolkit.h.flash_success(toolkit._('Tag suggestion option updated successfully.'))

                elif action == 'freq':
                    update_freq = request.form.get(UPDATE_FREQUENCY_KEY, None)
                    try:
                        update_freq = int(update_freq)
                        logic.get_action(u'config_option_update')({
                            u'user': toolkit.c.user
                        }, {
                            UPDATE_FREQUENCY_KEY: update_freq
                        })
                        try:
                            scheduler = Scheduler()
                            scheduler.update_interval()
                        except Exception:
                            toolkit.h.flash_error(toolkit._(
                                'The update frequency was not updated due to an error rescheduling the background job.'
                            ))
                        toolkit.h.flash_success(toolkit._('Update frequency has been updated successfully.'))
                    except (ValueError, TypeError):
                        toolkit.h.flash_error(toolkit._(
                            'The update frequency is specified in full minutes. Please provide a value that can be parsed as an integer.'
                        ))

            available = get_available_ontologies()
            title_lookup = {o['id']: o['text'] for o in available}
            preselected_ontologies = [{'id': oid, 'text': title_lookup.get(oid, oid)} for oid in config.get(ONTOLOGIES_KEY, '').strip().split() if oid]


            return toolkit.render('admin_semantictags.jinja2',
                                  extra_vars={
                                      'ontologies': (config.get(ONTOLOGIES_KEY) or '').strip(),
                                      'preselected_ontologies': preselected_ontologies,
                                      'free_tags': (config.get(FREE_TAGS_KEY) or 'false').lower(),
                                      'force_reload': (config.get(FORCE_RELOAD_KEY) or 'false').lower(),
                                      'tag_suggest': (config.get(TAG_SUGGEST_KEY) or 'false').lower(),
                                      'freq': config.get(UPDATE_FREQUENCY_KEY),
                                  })

        return blueprint
