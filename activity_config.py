"""
Configurable activity classification rules.
"""

from datetime import datetime

from database.models import ActivityRule


class ActivityConfigManager:
    def __init__(self, db_session):
        self.session = db_session
        self._ensure_default_rules()

    def _ensure_default_rules(self):
        if self.session.query(ActivityRule).count() > 0:
            return

        defaults = [
            ActivityRule(department="factories", business_type="factory", active_window_months=12, dormant_window_months=24),
            ActivityRule(department="shop_establishment", business_type="shop", active_window_months=12, dormant_window_months=18),
            ActivityRule(department="kspcb", business_type="industrial", active_window_months=12, dormant_window_months=24),
            ActivityRule(department="labour", business_type="all", active_window_months=12, dormant_window_months=24),
            ActivityRule(department="default", business_type="all", active_window_months=12, dormant_window_months=24),
        ]

        for rule in defaults:
            self.session.add(rule)
        self.session.commit()

    def get_rule(self, department, business_type="all"):
        department = (department or "default").lower()
        business_type = (business_type or "all").lower()

        rule = (
            self.session.query(ActivityRule)
            .filter_by(department=department, business_type=business_type)
            .first()
        )
        if rule:
            return rule

        rule = (
            self.session.query(ActivityRule)
            .filter_by(department=department, business_type="all")
            .first()
        )
        if rule:
            return rule

        rule = (
            self.session.query(ActivityRule)
            .filter_by(department="default", business_type="all")
            .first()
        )
        if rule:
            return rule

        # Final in-memory fallback.
        return ActivityRule(department="default", business_type="all", active_window_months=12, dormant_window_months=24)

    def get_all_rules(self):
        return self.session.query(ActivityRule).order_by(ActivityRule.department.asc(), ActivityRule.business_type.asc()).all()

    def update_rule(self, rule_id, updates):
        rule = self.session.query(ActivityRule).get(rule_id)
        if not rule:
            return None

        allowed = {
            "department",
            "business_type",
            "active_window_months",
            "dormant_window_months",
            "min_consumption_kwh",
            "min_consumption_liters",
            "renewal_weight",
            "inspection_weight",
            "compliance_filing_weight",
            "consumption_weight",
            "notice_weight",
        }
        for key, value in updates.items():
            if key in allowed:
                setattr(rule, key, value)

        rule.updated_at = datetime.utcnow()
        self.session.commit()
        return rule

    def create_rule(self, rule_data):
        rule = ActivityRule(**rule_data)
        self.session.add(rule)
        self.session.commit()
        return rule
