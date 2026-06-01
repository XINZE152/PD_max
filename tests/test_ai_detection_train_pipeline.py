import unittest

from app.ai_detection.train_pipeline_v2 import TrainPipeline


class TrainPipelineFeedbackLabelTests(unittest.TestCase):
    def test_wrong_feedback_after_tamper_prediction_means_normal(self):
        label = TrainPipeline.infer_feedback_label(
            {
                "judgment": "wrong",
                "engine_result": {"result": "篡改"},
            }
        )
        self.assertEqual(label, 0)

    def test_wrong_feedback_after_normal_prediction_means_tampered(self):
        label = TrainPipeline.infer_feedback_label(
            {
                "judgment": "wrong",
                "engine_result": {"result": "正常"},
            }
        )
        self.assertEqual(label, 1)

    def test_correct_feedback_keeps_engine_label(self):
        tampered = TrainPipeline.infer_feedback_label(
            {
                "judgment": "correct",
                "engine_result": {"result": "可疑"},
            }
        )
        normal = TrainPipeline.infer_feedback_label(
            {
                "judgment": "correct",
                "engine_result": {"result": "正常"},
            }
        )
        self.assertEqual(tampered, 1)
        self.assertEqual(normal, 0)

    def test_unconfirmed_suspicious_is_not_used_for_training(self):
        label = TrainPipeline.infer_feedback_label(
            {
                "judgment": "suspicious",
                "engine_result": {"result": "篡改"},
            }
        )
        self.assertIsNone(label)

    def test_missing_engine_result_is_not_guessed(self):
        label = TrainPipeline.infer_feedback_label(
            {
                "judgment": "wrong",
                "engine_result": {},
            }
        )
        self.assertIsNone(label)


if __name__ == "__main__":
    unittest.main()
