from __future__ import annotations

from apps.g2b_summary.service.graph import build_summary_graph_definition


def build_text_extract_payload(*, job_id: str, bucket: str, object_key: str, callback_url: str | None) -> dict:
    return {
        "source": {
            "type": "s3",
            "endpoint_env": "G2B_SUMMARY_S3_ENDPOINT",
            "access_key_env": "G2B_SUMMARY_S3_ACCESS_KEY",
            "secret_key_env": "G2B_SUMMARY_S3_SECRET_KEY",
            "bucket": bucket,
            "secure_env": "G2B_SUMMARY_S3_SECURE",
            "object_key": object_key,
        },
        "target": {
            "database_url_env": "G2B_SUMMARY_DATABASE_URL",
            "schema": "summary",
            "table": "extracted_texts",
            "job_id": job_id,
            "text_column": "text",
            "key_column": "job_id",
        },
        "metadata": {
            "job_id": job_id,
            "callback_url": callback_url,
            "graph": build_summary_graph_definition(),
        },
    }


def build_llm_generate_payload(*, job_id: str, callback_url: str | None) -> dict:
    return {
        "request": {
            "provider": "mock",
            "model": "mock-llm",
            "operation": "summarize",
            "prompt_template": "다음 텍스트를 간단히 요약하세요.\n\n{text}",
            "max_output_chars": 600,
        },
        "source": {
            "type": "postgres",
            "database_url_env": "G2B_SUMMARY_DATABASE_URL",
            "schema": "summary",
            "table": "extracted_texts",
            "key_column": "job_id",
            "key_value": job_id,
            "text_column": "text",
        },
        "target": {
            "type": "postgres",
            "database_url_env": "G2B_SUMMARY_DATABASE_URL",
            "schema": "summary",
            "table": "results",
            "key_column": "job_id",
            "key_value": job_id,
            "output_text_column": "summary_text",
            "model_column": "model",
            "prompt_version_column": "prompt_version",
            "raw_result_column": "raw_result",
            "model": "mock-llm",
            "prompt_version": "sample-v1",
        },
        "metadata": {
            "job_id": job_id,
            "callback_url": callback_url,
            "graph": build_summary_graph_definition(),
        },
    }
