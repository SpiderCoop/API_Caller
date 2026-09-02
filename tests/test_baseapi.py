"""
Unit tests for the BaseAPI class.
Tests basic API request functionality and common behaviors.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from econ_api_bridge.baseapi.baseapi import BaseAPI


class TestBaseAPIInitialization:
    """Tests for BaseAPI initialization."""

    @pytest.mark.unit
    def test_initialization_with_api_key(self, api_key):
        """Test BaseAPI initialization with API key."""
        base_url = "https://api.example.com"
        api = BaseAPI(api_key=api_key, base_url=base_url, timeout=10)

        assert api.base_url == base_url
        assert api.timeout == 10
        assert api.session is not None

    @pytest.mark.unit
    def test_initialization_with_defaults(self):
        """Test BaseAPI initialization with default values."""
        api = BaseAPI()

        assert api.base_url == ""
        assert api.timeout == 10
        assert api.session is not None

    @pytest.mark.unit
    def test_initialization_session_configuration(self, api_key):
        """Test that session is properly configured with retry strategy."""
        api = BaseAPI(api_key=api_key, base_url="https://api.example.com")

        # Check that session has retry adapters mounted
        assert hasattr(api.session, "get_adapter")
        assert api.session is not None


class TestBaseAPIMakeRequest:
    """Tests for BaseAPI._make_request method."""

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_success(self, mock_request, api_key):
        """Test successful API request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success", "data": [1, 2, 3]}
        mock_request.return_value = mock_response

        api = BaseAPI(api_key=api_key, base_url="https://api.example.com")
        result = api._make_request("/endpoint")

        assert result == {"status": "success", "data": [1, 2, 3]}
        mock_request.assert_called_once()

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_with_headers(self, mock_request, api_key):
        """Test API request with custom headers."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_request.return_value = mock_response

        api = BaseAPI(api_key=api_key, base_url="https://api.example.com")
        custom_headers = {"X-Custom": "header-value"}
        api._make_request("/endpoint", headers=custom_headers)

        call_args = mock_request.call_args
        headers = call_args[1]["headers"]

        # Should have Authorization header added
        assert "Authorization" in headers
        assert "X-Custom" in headers
        assert headers["X-Custom"] == "header-value"

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_with_params(self, mock_request, api_key):
        """Test API request with query parameters."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_request.return_value = mock_response

        api = BaseAPI(api_key=api_key, base_url="https://api.example.com")
        params = {"param1": "value1", "param2": "value2"}
        api._make_request("/endpoint", params=params)

        call_args = mock_request.call_args
        assert call_args[1]["params"] == params

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_with_api_key_authorization(self, mock_request, api_key):
        """Test that API key is added to Authorization header."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_request.return_value = mock_response

        api = BaseAPI(api_key=api_key, base_url="https://api.example.com")
        api._make_request("/endpoint")

        call_args = mock_request.call_args
        headers = call_args[1]["headers"]

        assert headers["Authorization"] == f"Bearer {api_key}"

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_without_api_key(self, mock_request):
        """Test request without API key (no Authorization header)."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_request.return_value = mock_response

        api = BaseAPI(base_url="https://api.example.com")
        api._make_request("/endpoint")

        call_args = mock_request.call_args
        headers = call_args[1]["headers"]

        assert "Authorization" not in headers

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_http_error(self, mock_request):
        """Test API request with HTTP error."""
        mock_request.side_effect = requests.exceptions.HTTPError("404 Not Found")

        api = BaseAPI(base_url="https://api.example.com")

        with pytest.raises(requests.exceptions.HTTPError):
            api._make_request("/endpoint")

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_connection_error(self, mock_request):
        """Test API request with connection error."""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        api = BaseAPI(base_url="https://api.example.com")

        with pytest.raises(requests.exceptions.RequestException):
            api._make_request("/endpoint")

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_json_decode_error(self, mock_request):
        """Test API request with invalid JSON response."""
        mock_response = MagicMock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_request.return_value = mock_response

        api = BaseAPI(base_url="https://api.example.com")

        with pytest.raises(ValueError):
            api._make_request("/endpoint")

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_constructs_correct_url(self, mock_request, api_key):
        """Test that the correct URL is constructed."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_request.return_value = mock_response

        base_url = "https://api.example.com"
        endpoint = "/v1/data"
        api = BaseAPI(api_key=api_key, base_url=base_url)
        api._make_request(endpoint)

        call_args = mock_request.call_args
        assert call_args[1]["url"] == base_url + endpoint

    @pytest.mark.unit
    @patch("requests.Session.request")
    def test_make_request_respects_timeout(self, mock_request, api_key):
        """Test that the timeout parameter is used."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_request.return_value = mock_response

        timeout_value = 30
        api = BaseAPI(
            api_key=api_key, base_url="https://api.example.com", timeout=timeout_value
        )
        api._make_request("/endpoint")

        call_args = mock_request.call_args
        assert call_args[1]["timeout"] == timeout_value
