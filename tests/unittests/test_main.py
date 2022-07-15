import unittest
from unittest import mock
from tap_github import main
from tap_github.discover import discover


class MockArgs:
    """Mock args object class"""
    
    def __init__(self, config = None, properties = None, state = None, discover = False) -> None:
        self.config = config 
        self.properties = properties
        self.state = state
        self.discover = discover

@mock.patch("tap_github.GithubClient")
@mock.patch("singer.utils.parse_args")
class TestDiscoverMode(unittest.TestCase):
    """
    Test main function for discover mode
    """

    mock_config = {"start_date": "", "access_token": ""}
    
    @mock.patch("tap_github._discover")
    def test_discover_with_config(self, mock_discover, mock_args, mock_verify_access):
        """Test `_discover` function is called for discover mode"""
        mock_discover.return_value = dict()
        mock_args.return_value = MockArgs(discover = True, config = self.mock_config)
        main()
        
        self.assertTrue(mock_discover.called)


@mock.patch("tap_github.GithubClient")
@mock.patch("singer.utils.parse_args")
@mock.patch("tap_github._sync")
class TestSyncMode(unittest.TestCase):
    """
    Test main function for sync mode
    """

    mock_config = {"start_date": "", "access_token": ""}
    mock_catalog = {"streams": [{"stream": "teams", "schema": {}, "metadata": {}}]}

    @mock.patch("tap_github._discover")
    def test_sync_with_properties(self, mock_discover, mock_sync, mock_args, mock_client):
        """Test sync mode with properties given in args"""

        mock_client.return_value = "mock_client"
        mock_args.return_value = MockArgs(config=self.mock_config, properties=self.mock_catalog)
        main()
        
        # Verify `_sync` is called with ecpected arguments
        mock_sync.assert_called_with("mock_client", self.mock_config, {}, self.mock_catalog)
        
        # verify `_discover` function is not called
        self.assertFalse(mock_discover.called)

    @mock.patch("tap_github._discover")
    def test_sync_without_properties(self, mock_discover, mock_sync, mock_args, mock_client):
        """Test sync mode without properties given in args"""
        
        mock_discover.return_value = {"schema": "", "metadata": ""}
        mock_client.return_value = "mock_client"
        mock_args.return_value = MockArgs(config=self.mock_config)
        main()
        
        # Verify `_sync` is called with ecpected arguments
        mock_sync.assert_called_with("mock_client", self.mock_config, {}, {"schema": "", "metadata": ""})

        # verify `_discover` function is  called
        self.assertTrue(mock_discover.called)

    def test_sync_with_state(self, mock_sync, mock_args, mock_client):
        """Test sync mode with state gicen in args"""
        mock_state = {"bookmarks": {"projec ts": ""}}
        mock_client.return_value = "mock_client"
        mock_args.return_value = MockArgs(config=self.mock_config, properties=self.mock_catalog, state=mock_state)
        main()
        
        # Verify `_sync` is called with ecpected arguments
        mock_sync.assert_called_with("mock_client", self.mock_config, mock_state, self.mock_catalog)


class TestDiscover(unittest.TestCase):
    """Test `discover` function."""
    def test_discover(self):
        
        return_catalog = discover()
        
        self.assertIsInstance(return_catalog, dict)

    @mock.patch("tap_github.discover.Schema")
    @mock.patch("tap_github.discover.LOGGER.error")
    def test_discover_error_handling(self, mock_logger, mock_schema):
        """Test discover function if exception arises."""
        mock_schema.from_dict.side_effect = [Exception]
        with self.assertRaises(Exception):
            discover()

        # Verify logger called 3 times when exception arises.
        self.assertEqual(mock_logger.call_count, 3)