"""
Confidence calibration for entity resolution thresholds.
Learns from reviewer decisions and writes runtime thresholds.
"""

import json
import os
from datetime import datetime, timedelta

import numpy as np

from database.models import CalibrationLog, MatchCandidate, ReviewerFeedback, RetrainingRun

RUNTIME_THRESHOLDS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "runtime_thresholds.json")


class ConfidenceCalibrator:
    def __init__(self, db_session):
        self.session = db_session

    def estimate_thresholds_from_feedback(self, feedback_window_days=30):
        cutoff_date = datetime.utcnow() - timedelta(days=feedback_window_days)

        feedback = (
            self.session.query(ReviewerFeedback)
            .filter(ReviewerFeedback.decided_at >= cutoff_date)
            .all()
        )

        scored_feedback = []
        for fb in feedback:
            score = fb.confidence_at_decision
            if score is None and fb.match_id:
                mc = self.session.query(MatchCandidate).get(fb.match_id)
                score = mc.similarity_score if mc else None

            if score is None:
                continue

            verdict = self._to_verdict(fb.decision)
            if verdict is None:
                continue

            scored_feedback.append((float(score), verdict))

        if len(scored_feedback) < 50:
            return self._get_default_thresholds()

        match_scores = [s for s, v in scored_feedback if v == "match"]
        nonmatch_scores = [s for s, v in scored_feedback if v == "nonmatch"]

        if not match_scores or not nonmatch_scores:
            return self._get_default_thresholds()

        auto_threshold = max(
            np.percentile(match_scores, 85),
            np.percentile(nonmatch_scores, 99) + 0.05,
        )

        review_lower = np.percentile(nonmatch_scores, 90)
        review_upper = auto_threshold

        auto_threshold = float(min(0.98, max(0.70, auto_threshold)))
        review_lower = float(max(0.30, min(review_lower, auto_threshold - 0.01)))

        return {
            "auto_link": round(auto_threshold, 3),
            "review_lower": round(review_lower, 3),
            "review_upper": round(review_upper, 3),
            "precision_estimate": self._estimate_precision(match_scores, nonmatch_scores),
            "recall_estimate": self._estimate_recall(match_scores, nonmatch_scores),
            "feedback_samples": len(scored_feedback),
        }

    def _to_verdict(self, decision):
        if decision == "merge":
            return "match"
        if decision == "reject":
            return "nonmatch"
        return None

    def _get_default_thresholds(self):
        return {
            "auto_link": 0.92,
            "review_lower": 0.65,
            "review_upper": 0.92,
            "precision_estimate": None,
            "recall_estimate": None,
            "feedback_samples": 0,
        }

    def _estimate_precision(self, match_scores, nonmatch_scores):
        auto_threshold = np.percentile(match_scores, 85)
        false_positives = sum(1 for s in nonmatch_scores if s >= auto_threshold)
        true_positives = sum(1 for s in match_scores if s >= auto_threshold)

        total = true_positives + false_positives
        if total == 0:
            return None
        return round(true_positives / total, 3)

    def _estimate_recall(self, match_scores, _nonmatch_scores):
        auto_threshold = np.percentile(match_scores, 85)
        true_positives = sum(1 for s in match_scores if s >= auto_threshold)

        if not match_scores:
            return None
        return round(true_positives / len(match_scores), 3)

    def suggest_threshold_adjustments(self):
        current = self.get_current_thresholds()
        recommended = self.estimate_thresholds_from_feedback()

        return {
            "current": current,
            "recommended": recommended,
            "changes": {
                "auto_link": round(recommended["auto_link"] - current["auto_link"], 3),
                "review_lower": round(recommended["review_lower"] - current["review_lower"], 3),
            },
            "action_needed": abs(recommended["auto_link"] - current["auto_link"]) > 0.05,
        }

    def get_current_thresholds(self):
        if os.path.exists(RUNTIME_THRESHOLDS_FILE):
            try:
                with open(RUNTIME_THRESHOLDS_FILE, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                return {
                    "auto_link": float(payload.get("auto_link", 0.92)),
                    "review_lower": float(payload.get("review_lower", 0.65)),
                    "review_upper": float(payload.get("review_upper", 0.92)),
                }
            except (ValueError, OSError, json.JSONDecodeError):
                pass

        latest = self.session.query(CalibrationLog).order_by(CalibrationLog.id.desc()).first()
        if latest:
            return {
                "auto_link": latest.auto_threshold,
                "review_lower": latest.review_lower,
                "review_upper": latest.review_upper,
            }

        return {
            "auto_link": 0.92,
            "review_lower": 0.65,
            "review_upper": 0.92,
        }

    def apply_recommended_thresholds(self, applied_by="system"):
        current = self.get_current_thresholds()
        recommended = self.estimate_thresholds_from_feedback()

        log = CalibrationLog(
            auto_threshold=recommended["auto_link"],
            review_lower=recommended["review_lower"],
            review_upper=recommended["review_upper"],
            precision_estimate=recommended.get("precision_estimate"),
            recall_estimate=recommended.get("recall_estimate"),
            feedback_samples=recommended.get("feedback_samples", 0),
        )
        self.session.add(log)

        retraining = RetrainingRun(
            old_auto_threshold=current["auto_link"],
            new_auto_threshold=recommended["auto_link"],
            old_review_lower=current["review_lower"],
            new_review_lower=recommended["review_lower"],
            precision_before=None,
            precision_after=recommended.get("precision_estimate"),
            recall_before=None,
            recall_after=recommended.get("recall_estimate"),
            feedback_count_used=recommended.get("feedback_samples", 0),
            applied_by=applied_by,
        )
        self.session.add(retraining)
        self.session.commit()

        with open(RUNTIME_THRESHOLDS_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "auto_link": recommended["auto_link"],
                    "review_lower": recommended["review_lower"],
                    "review_upper": recommended["review_upper"],
                    "applied_at": datetime.utcnow().isoformat(),
                    "applied_by": applied_by,
                },
                f,
                indent=2,
            )

        return recommended
