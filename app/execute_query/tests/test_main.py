import pytest
from google.cloud.exceptions import Forbidden, NotFound
from src.main import execute_query, load_query_from_gcs

# =============================================================================
# load_query_from_gcs 関数のテスト
# =============================================================================


def test_load_query_from_gcs_success(mocker):  # noqa: ANN001, ANN201
    """正常系: バケットとファイルが存在する場合のテスト."""
    # モックオブジェクトを作成
    mock_storage_client = mocker.Mock()
    mock_bucket = mocker.Mock()
    mock_blob = mocker.Mock()

    # バケットとファイルのモックの返り値を設定
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.download_as_string.return_value = b"SELECT * FROM test_table;"

    # 関数を実行して結果を確認
    result = load_query_from_gcs(mock_storage_client, "test_bucket", "test_file.sql")
    assert result == "SELECT * FROM test_table;"

    # モックが正しく呼び出されたことを確認
    mock_storage_client.bucket.assert_called_once_with("test_bucket")
    mock_bucket.blob.assert_called_once_with("test_file.sql")
    mock_blob.download_as_string.assert_called_once()


def test_load_query_from_gcs_not_found(mocker):  # noqa: ANN001, ANN201
    """404 Not Found エラーが発生する場合のテスト."""
    # モックオブジェクトを作成
    mock_storage_client = mocker.Mock()
    mock_bucket = mocker.Mock()

    # NotFound エラーを発生させる
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.side_effect = NotFound("Bucket or file not found")

    # 例外が正しく発生するかを確認
    with pytest.raises(NotFound):
        load_query_from_gcs(mock_storage_client, "test_bucket", "non_existent_file.sql")

    # モックが正しく呼び出されたことを確認
    mock_storage_client.bucket.assert_called_once_with("test_bucket")
    mock_bucket.blob.assert_called_once_with("non_existent_file.sql")

