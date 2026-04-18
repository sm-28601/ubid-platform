from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class SourceRecord(Base):
    __tablename__ = 'source_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_system = Column(String, nullable=False, index=True)
    source_id = Column(String, nullable=False)
    raw_name = Column(Text)
    normalized_name = Column(Text)
    raw_address = Column(Text)
    normalized_address = Column(Text)
    pincode = Column(String, index=True)
    pan = Column(String, index=True)
    gstin = Column(String, index=True)
    owner_name = Column(Text)
    registration_date = Column(String)
    category = Column(String)
    raw_json = Column(Text)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class UbidMaster(Base):
    __tablename__ = 'ubid_master'

    ubid = Column(String, primary_key=True)
    anchor_type = Column(String)
    anchor_value = Column(String)
    canonical_name = Column(Text)
    canonical_address = Column(Text)
    pincode = Column(String)
    activity_status = Column(String, default="Unknown")
    status_updated_at = Column(DateTime)
    status_evidence = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class UbidLinkage(Base):
    __tablename__ = 'ubid_linkages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ubid = Column(String, ForeignKey('ubid_master.ubid'), nullable=False, index=True)
    source_system = Column(String, nullable=False)
    source_id = Column(String, nullable=False)
    confidence_score = Column(Float)
    match_evidence = Column(Text)
    linked_by = Column(String, default="system")
    linked_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)

class MatchCandidate(Base):
    __tablename__ = 'match_candidates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    record_a_id = Column(Integer, ForeignKey('source_records.id'), nullable=False)
    record_b_id = Column(Integer, ForeignKey('source_records.id'), nullable=False)
    similarity_score = Column(Float)
    match_evidence = Column(Text)
    status = Column(String, default="pending", index=True)
    reviewed_by = Column(String)
    reviewed_at = Column(DateTime)
    reviewer_notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

class ActivityEvent(Base):
    __tablename__ = 'activity_events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_system = Column(String, nullable=False)
    source_event_id = Column(String)
    event_type = Column(String, nullable=False)
    event_date = Column(String, nullable=False, index=True)
    event_details = Column(Text)
    matched_ubid = Column(String, index=True)
    match_confidence = Column(Float)
    raw_identifier = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

class AuditLog(Base):
    __tablename__ = 'audit_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    action_type = Column(String, nullable=False)
    ubid = Column(String)
    details = Column(Text)
    performed_by = Column(String, default="system")
    performed_at = Column(DateTime, default=datetime.utcnow)

class ReviewerFeedback(Base):
    __tablename__ = 'reviewer_feedback'

    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey('match_candidates.id'))
    decision = Column(String, nullable=False)
    confidence_at_decision = Column(Float)
    reviewer_notes = Column(Text)
    decided_at = Column(DateTime, default=datetime.utcnow)


class RetrainingRun(Base):
    __tablename__ = 'retraining_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_date = Column(DateTime, default=datetime.utcnow)
    old_auto_threshold = Column(Float)
    new_auto_threshold = Column(Float)
    old_review_lower = Column(Float)
    new_review_lower = Column(Float)
    precision_before = Column(Float)
    precision_after = Column(Float)
    recall_before = Column(Float)
    recall_after = Column(Float)
    feedback_count_used = Column(Integer, default=0)
    applied_by = Column(String)


class CalibrationLog(Base):
    __tablename__ = 'calibration_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    auto_threshold = Column(Float)
    review_lower = Column(Float)
    review_upper = Column(Float)
    precision_estimate = Column(Float)
    recall_estimate = Column(Float)
    feedback_samples = Column(Integer, default=0)


class FeedbackProcessingLog(Base):
    __tablename__ = 'feedback_processing_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    feedback_id = Column(Integer, ForeignKey('reviewer_feedback.id'), nullable=False, unique=True)
    processed_at = Column(DateTime, default=datetime.utcnow)
    result_type = Column(String, nullable=False)
    details = Column(Text)


class FeatureWeight(Base):
    __tablename__ = 'feature_weights'

    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String, nullable=False, unique=True)
    weight = Column(Float, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ActivityRule(Base):
    __tablename__ = 'activity_rules'

    id = Column(Integer, primary_key=True, autoincrement=True)
    department = Column(String, nullable=False)
    business_type = Column(String, nullable=False, default='all')
    active_window_months = Column(Integer, default=12)
    dormant_window_months = Column(Integer, default=24)
    min_consumption_kwh = Column(Float, default=100.0)
    min_consumption_liters = Column(Float, default=1000.0)
    renewal_weight = Column(Float, default=1.0)
    inspection_weight = Column(Float, default=0.8)
    compliance_filing_weight = Column(Float, default=0.9)
    consumption_weight = Column(Float, default=0.6)
    notice_weight = Column(Float, default=-0.5)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class GoldenRecord(Base):
    __tablename__ = 'golden_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ubid = Column(String, ForeignKey('ubid_master.ubid'), nullable=False, unique=True, index=True)
    golden_name = Column(Text)
    golden_address = Column(Text)
    golden_pan = Column(String)
    golden_gstin = Column(String)
    golden_owner = Column(Text)
    golden_pincode = Column(String)
    survivorship_rule = Column(String, default='latest_verified')
    evidence = Column(Text)
    updated_by = Column(String, default='system')
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ReviewEscalation(Base):
    __tablename__ = 'review_escalations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey('match_candidates.id'), nullable=False, index=True)
    reason_code = Column(String, nullable=False)
    escalated_to = Column(String, default='supervisor')
    escalated_by = Column(String, default='reviewer')
    notes = Column(Text)
    status = Column(String, default='open', index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime)


class WatchlistAlert(Base):
    __tablename__ = 'watchlist_alerts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    alert_type = Column(String, nullable=False, index=True)
    severity = Column(String, default='medium')
    entity_ref = Column(String)
    title = Column(String, nullable=False)
    details = Column(Text)
    status = Column(String, default='open', index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class WebhookSubscription(Base):
    __tablename__ = 'webhook_subscriptions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    target_url = Column(String, nullable=False)
    event_type = Column(String, nullable=False, index=True)
    is_active = Column(Boolean, default=True)
    secret = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class ReportSchedule(Base):
    __tablename__ = 'report_schedules'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    report_type = Column(String, nullable=False)
    cron_expression = Column(String, nullable=False)
    destination = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    last_run_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
