"""
Learns from reviewer feedback by adjusting feature weights.
"""

import json
import os
from datetime import datetime

from database.models import (
    FeatureWeight,
    FeedbackProcessingLog,
    MatchCandidate,
    ReviewerFeedback,
)

RUNTIME_WEIGHTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feature_weights.json")


class FeedbackLearner:
    def __init__(self, db_session):
        self.session = db_session
        self.feature_weights = self._load_weights()

    def _default_weights(self):
        return {
            "pan_match": 0.35,
            "gstin_match": 0.30,
            "name_similarity": 0.15,
            "address_similarity": 0.10,
            "pincode_match": 0.05,
            "owner_similarity": 0.05,
        }

    def _load_weights(self):
        rows = self.session.query(FeatureWeight).all()
        if rows:
            weights = {r.feature_name: float(r.weight) for r in rows}
            return self._normalize(weights)

        if os.path.exists(RUNTIME_WEIGHTS_FILE):
            try:
                with open(RUNTIME_WEIGHTS_FILE, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                return self._normalize(payload)
            except (ValueError, OSError, json.JSONDecodeError):
                pass

        return self._default_weights()

    def process_unapplied_feedback(self):
        processed_ids = {
            row.feedback_id
            for row in self.session.query(FeedbackProcessingLog.feedback_id).all()
        }

        feedbacks = (
            self.session.query(ReviewerFeedback)
            .filter(ReviewerFeedback.decision.in_(["merge", "reject"]))
            .order_by(ReviewerFeedback.id.asc())
            .all()
        )

        results = []
        for feedback in feedbacks:
            if feedback.id in processed_ids:
                continue

            result = self._process_single_feedback(feedback)
            results.append(result)

            self.session.add(
                FeedbackProcessingLog(
                    feedback_id=feedback.id,
                    result_type=result.get("type", "unknown"),
                    details=json.dumps(result),
                )
            )

        self._save_weights()
        self.session.commit()
        return results

    def _process_single_feedback(self, feedback):
        candidate = self.session.query(MatchCandidate).get(feedback.match_id)
        if not candidate:
            return {
                "feedback_id": feedback.id,
                "type": "missing_candidate",
            }

        score = feedback.confidence_at_decision
        if score is None:
            score = candidate.similarity_score or 0.0

        evidence = {}
        if candidate.match_evidence:
            try:
                evidence = json.loads(candidate.match_evidence)
            except json.JSONDecodeError:
                evidence = {}

        high_features = self._extract_high_features(evidence)

        if feedback.decision == "reject" and score > 0.8:
            self._adjust_weights(high_features, direction="decrease")
            return {
                "feedback_id": feedback.id,
                "type": "false_positive_corrected",
                "features_adjusted": high_features,
                "score": score,
            }

        if feedback.decision == "merge" and score < 0.6:
            self._adjust_weights(high_features, direction="increase")
            return {
                "feedback_id": feedback.id,
                "type": "false_negative_corrected",
                "features_adjusted": high_features,
                "score": score,
            }

        return {
            "feedback_id": feedback.id,
            "type": "consistent",
            "score": score,
        }

    def _extract_high_features(self, evidence):
        keys = [
            "pan_match",
            "gstin_match",
            "name_similarity",
            "address_similarity",
            "pincode_match",
            "owner_similarity",
        ]
        high = []
        for key in keys:
            data = evidence.get(key, {})
            score = data.get("score", 0.0)
            if isinstance(score, (int, float)) and score >= 0.75:
                high.append(key)
        return high

    def _adjust_weights(self, features, direction="decrease"):
        if not features:
            return

        adjustment = -0.02 if direction == "decrease" else 0.02
        for feature in features:
            if feature in self.feature_weights:
                old_val = float(self.feature_weights[feature])
                self.feature_weights[feature] = max(0.01, min(0.5, old_val + adjustment))

        self.feature_weights = self._normalize(self.feature_weights)

    def _normalize(self, weights):
        merged = self._default_weights()
        for key in merged:
            if key in weights:
                merged[key] = float(weights[key])

        total = sum(merged.values())
        if total <= 0:
            return self._default_weights()
        for key in merged:
            merged[key] = merged[key] / total
        return merged

    def _save_weights(self):
        for key, value in self.feature_weights.items():
            row = self.session.query(FeatureWeight).filter_by(feature_name=key).first()
            if row:
                row.weight = value
                row.updated_at = datetime.utcnow()
            else:
                self.session.add(FeatureWeight(feature_name=key, weight=value))

        with open(RUNTIME_WEIGHTS_FILE, "w", encoding="utf-8") as f:
            json.dump(self.feature_weights, f, indent=2)

    def get_weight_summary(self):
        return {
            "weights": self.feature_weights,
            "last_updated": datetime.utcnow().isoformat(),
        }
