import datetime
import logging
from sqlalchemy import Column, DateTime, String, Text, func, or_
from sqlalchemy.ext.declarative import declarative_base

import ckan.model as model

log = logging.getLogger(__name__)
Base = declarative_base()


class UMLSTagCache(Base):
    __tablename__ = 'semantictags_umls_cache'

    cui = Column(String(32), primary_key=True)
    ckan_tag_name = Column(String(100), nullable=False, index=True)
    preferred_name = Column(Text, nullable=False)
    iri = Column(Text, nullable=False, unique=True)
    definition = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow,
        nullable=False,
    )


def init_table():
    Base.metadata.create_all(bind=model.meta.engine, checkfirst=True)
    log.debug('Initialized semantictags_umls_cache table')


class UMLSCacheQuery:
    @staticmethod
    def get_by_cui(cui):
        return model.Session.query(UMLSTagCache).filter_by(cui=cui).first()

    @staticmethod
    def get_by_iri(iri):
        return model.Session.query(UMLSTagCache).filter_by(iri=iri).first()

    @staticmethod
    def get_by_tag_name(tag_name):
        if not tag_name:
            return None
        return (
            model.Session.query(UMLSTagCache)
            .filter(func.lower(UMLSTagCache.ckan_tag_name) == tag_name.strip().lower())
            .first()
        )

    @staticmethod
    def search(query, limit=10):
        pattern = f'%{(query or "").strip()}%'
        if pattern == '%%':
            return []
        return (
            model.Session.query(UMLSTagCache)
            .filter(
                or_(
                    UMLSTagCache.ckan_tag_name.ilike(pattern),
                    UMLSTagCache.preferred_name.ilike(pattern),
                    UMLSTagCache.cui.ilike(pattern),
                )
            )
            .limit(limit)
            .all()
        )

    @staticmethod
    def upsert(cui, ckan_tag_name, preferred_name, iri, definition=None):
        row = UMLSCacheQuery.get_by_cui(cui)
        if row:
            row.ckan_tag_name = ckan_tag_name
            row.preferred_name = preferred_name
            row.iri = iri
            if definition is not None:
                row.definition = definition
            row.updated_at = datetime.datetime.utcnow()
        else:
            row = UMLSTagCache(
                cui=cui,
                ckan_tag_name=ckan_tag_name,
                preferred_name=preferred_name,
                iri=iri,
                definition=definition,
            )
            model.Session.add(row)
        model.Session.commit()
        return row
    
