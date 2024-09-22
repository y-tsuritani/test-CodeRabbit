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


def test_load_query_from_gcs_forbidden(mocker):  # noqa: ANN001, ANN201
    """403 Forbidden エラーのテスト."""
    # モックオブジェクトを作成
    mock_storage_client = mocker.Mock()
    mock_bucket = mocker.Mock()

    # Forbidden エラーを発生させる
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.side_effect = Forbidden("Access denied")

    # 例外が正しく発生するかを確認
    with pytest.raises(Forbidden):
        load_query_from_gcs(mock_storage_client, "test_bucket", "restricted_file.sql")

    # モックが正しく呼び出されたことを確認
    mock_storage_client.bucket.assert_called_once_with("test_bucket")
    mock_bucket.blob.assert_called_once_with("restricted_file.sql")


def test_load_query_from_gcs_unexpected_error(mocker):  # noqa: ANN001, ANN201
    """予期しないエラーが発生する場合のテスト."""
    # モックオブジェクトを作成
    mock_storage_client = mocker.Mock()
    mock_bucket = mocker.Mock()

    # 予期しないエラーを発生させる
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.side_effect = RuntimeError("Unexpected error")

    # 例外が正しく発生するかを確認
    with pytest.raises(RuntimeError, match="An unexpected error occurred"):
        load_query_from_gcs(mock_storage_client, "test_bucket", "some_file.sql")

    # モックが正しく呼び出されたことを確認
    mock_storage_client.bucket.assert_called_once_with("test_bucket")
    mock_bucket.blob.assert_called_once_with("some_file.sql")


# =============================================================================
# execute_query 関数のテスト
# =============================================================================


def test_execute_query_success(mocker):  # noqa: ANN001, ANN201
    """正常系: クエリの実行が成功する場合のテスト."""
    # モックオブジェクトを作成
    mock_bq_client = mocker.Mock()
    mock_query_job = mocker.Mock()
    mock_query_job.result.return_value = None

    # クエリの実行のモックの返り値を設定
    mock_bq_client.query.return_value = mock_query_job
    test_query = "SELECT * FROM `test_dataset.test_table`;"
    mock_table_ref = "test_project.test_dataset.test_table"
    # 関数を実行して結果を確認
    execute_query(mock_bq_client, test_query, mock_table_ref)

    # モックが正しく呼び出されたことを確認
    mock_bq_client.query.assert_called_once_with(test_query, job_config=mocker.ANY)
    mock_query_job.result.assert_called_once()


def test_execute_query_not_found(mocker):  # noqa: ANN001, ANN201
    """404 Not Found エラーが発生する場合のテスト."""
    # モックオブジェクトを作成
    mock_bq_client = mocker.Mock()

    test_query = "SELECT * FROM `test_dataset.test_table`;"
    mock_table_ref = "test_project.test_dataset.test_table"

    # NotFound エラーを発生させる
    mock_bq_client.query.side_effect = NotFound("Table not found")

    # 例外が正しく発生するかを確認
    with pytest.raises(NotFound):
        execute_query(mock_bq_client, test_query, mock_table_ref)

    # モックが正しく呼び出されたことを確認
    mock_bq_client.query.assert_called_once_with(test_query, job_config=mocker.ANY)


def test_execute_query_forbidden(mocker):  # noqa: ANN001, ANN201
    """403 Forbidden エラーのテスト."""
    # モックオブジェクトを作成
    mock_bq_client = mocker.Mock()

    test_query = "SELECT * FROM `test_dataset.test_table`;"
    mock_table_ref = "test_project.test_dataset.test_table"

    # Forbidden エラーを発生させる
    mock_bq_client.query.side_effect = Forbidden("Access denied")

    # 例外が正しく発生するかを確認
    with pytest.raises(Forbidden):
        execute_query(mock_bq_client, test_query, mock_table_ref)

    # モックが正しく呼び出されたことを確認
    mock_bq_client.query.assert_called_once_with(test_query, job_config=mocker.ANY)


def test_execute_query_unexpected_error(mocker):  # noqa: ANN001, ANN201
    """予期しないエラーが発生する場合のテスト."""
    # モックオブジェクトを作成
    mock_bq_client = mocker.Mock()

    test_query = "SELECT * FROM `test_dataset.test_table`;"
    mock_table_ref = "test_project.test_dataset.test_table"

    # 予期しないエラーを発生させる
    mock_bq_client.query.side_effect = RuntimeError("Unexpected error")

    # 例外が正しく発生するかを確認
    with pytest.raises(RuntimeError, match="An unexpected error occurred"):
        execute_query(mock_bq_client, test_query, mock_table_ref)

    # モックが正しく呼び出されたことを確認
    mock_bq_client.query.assert_called_once_with(test_query, job_config=mocker.ANY)
