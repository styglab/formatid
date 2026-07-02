import os
import unittest

try:
    from services.context_platform.internal.ingestion.operation_verification import _build_request_parts
    from services.context_platform.internal.ingestion.operation_verification import _method_allowed_for_verification
    from services.context_platform.internal.ingestion.operation_verification import _normalize_response_path
    from services.context_platform.internal.ingestion.operation_verification import _sample_parameter_values
except ModuleNotFoundError as exc:
    if exc.name != "httpx":
        raise
    HAS_HTTPX = False
else:
    HAS_HTTPX = True


@unittest.skipUnless(HAS_HTTPX, "httpx is not installed in this host test environment")
class OperationVerificationTest(unittest.TestCase):
    def test_sample_values_use_source_config_and_secret_env(self) -> None:
        os.environ["CONTEXT_PLATFORM_TEST_SERVICE_KEY"] = "secret-value"
        try:
            source = {
                "config": {
                    "verification": {
                        "secret_env": {"serviceKey": "CONTEXT_PLATFORM_TEST_SERVICE_KEY"},
                        "sample_parameters": {
                            "default": {
                                "법인등록번호": "1301110006246",
                                "사업연도": "2024",
                            }
                        },
                    }
                }
            }
            operation = {
                "name": "getSummFinaStat_V2",
                "parameters": [
                    {"name": "법인등록번호", "is_required": True},
                    {"name": "사업연도", "is_required": True},
                ],
            }

            values, missing = _sample_parameter_values(source, operation, operation["parameters"])

            self.assertEqual(values["법인등록번호"], "1301110006246")
            self.assertEqual(values["사업연도"], "2024")
            self.assertEqual(values["serviceKey"], "secret-value")
            self.assertEqual(missing, [])
        finally:
            os.environ.pop("CONTEXT_PLATFORM_TEST_SERVICE_KEY", None)

    def test_post_verification_requires_explicit_allow_method(self) -> None:
        source_config = {"verification": {"allow_methods": ["POST"]}}
        operation = {"method": "POST", "endpoint_metadata": {}}

        self.assertTrue(_method_allowed_for_verification(source_config, operation, "POST"))
        self.assertFalse(_method_allowed_for_verification({"verification": {}}, operation, "POST"))

    def test_build_request_parts_preserves_query_header_and_nested_json_body(self) -> None:
        operation = {
            "parameters": [
                {
                    "name": "serviceKey",
                    "location": "query",
                    "parameter_path": "request.query.serviceKey",
                    "data_type": "string",
                },
                {
                    "name": "Authorization",
                    "location": "header",
                    "parameter_path": "request.header.Authorization",
                    "data_type": "string",
                },
                {
                    "name": "b_no",
                    "location": "body",
                    "parameter_path": "request.body.businesses[].b_no",
                    "data_type": "string",
                },
                {
                    "name": "start_dt",
                    "location": "body",
                    "parameter_path": "request.body.businesses[].start_dt",
                    "data_type": "string",
                },
            ]
        }

        parts = _build_request_parts(
            operation,
            {
                "serviceKey": "secret-service-key",
                "Authorization": "secret-auth",
                "b_no": "1234567890",
                "start_dt": "20200101",
            },
        )

        self.assertEqual(parts["query"], {"serviceKey": "secret-service-key"})
        self.assertEqual(parts["headers"], {"Authorization": "secret-auth"})
        self.assertEqual(
            parts["body"],
            {"businesses": [{"b_no": "1234567890", "start_dt": "20200101"}]},
        )

    def test_normalize_response_path_treats_empty_array_marker_as_item(self) -> None:
        self.assertEqual(
            _normalize_response_path("response.body.data[].request_param.b_no"),
            "data.item.request_param.b_no",
        )


if __name__ == "__main__":
    unittest.main()
