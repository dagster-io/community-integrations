from unittest import mock

from dagster_iceberg._utils import properties


def test_table_property_differ_removed_properties():
    properties_current = {
        "a": "b",
        "c": "d",
        "e": "f",
    }
    properties_new = {
        "a": "b",
        "e": "f",
    }

    schema_differ = properties.TablePropertiesDiffer(
        current_table_properties=properties_current,
        new_table_properties=properties_new,
    )
    assert schema_differ.has_changes
    assert schema_differ.deleted_properties == ["c"]


def test_table_property_differ_removed_add_properties():
    properties_current = {
        "a": "b",
        "c": "d",
        "e": "f",
    }
    properties_new = {
        "a": "b",
        "c": "d",
        "e": "f",
        "g": "h",
    }

    table_property_differ = properties.TablePropertiesDiffer(
        current_table_properties=properties_current,
        new_table_properties=properties_new,
    )
    assert table_property_differ.has_changes
    assert table_property_differ.new_properties == ["g"]


def test_schema_differ_updated_properties():
    properties_current = {
        "a": "b",
        "c": "d",
        "e": "f",
    }
    properties_new = {
        "a": "b",
        "c": "d",
        "e": "update",
    }

    table_property_differ = properties.TablePropertiesDiffer(
        current_table_properties=properties_current,
        new_table_properties=properties_new,
    )
    assert table_property_differ.has_changes
    assert table_property_differ.updated_properties == ["e"]


def test_table_property_differ_many_change_properties():
    properties_current = {"a": "b", "c": "d"}
    # Add, delete, and update
    properties_new = {
        "a": "update",
        "e": "f",
    }

    table_property_differ = properties.TablePropertiesDiffer(
        current_table_properties=properties_current,
        new_table_properties=properties_new,
    )
    assert table_property_differ.has_changes
    assert table_property_differ.updated_properties == ["a"]
    assert table_property_differ.deleted_properties == ["c"]
    assert table_property_differ.new_properties == ["e"]


def test_iceberg_table_property_updater_many_changes():
    properties_current = {"a": "b", "c": "d"}
    # Add, delete, and update
    properties_new = {
        "a": "update",
        "e": "f",
    }
    table_property_updater = properties.IcebergTablePropertiesUpdater(
        table_properties_differ=properties.TablePropertiesDiffer(
            current_table_properties=properties_current,
            new_table_properties=properties_new,
        ),
    )
    mock_iceberg_table = mock.MagicMock()
    table_property_updater.update_table_properties(
        table=mock_iceberg_table,
        table_properties=properties_new,
    )
    mock_iceberg_table.transaction.assert_called_once()
    mock_iceberg_table.transaction.return_value.__enter__.return_value.remove_properties.assert_called_once_with(
        *["c"],
    )
    mock_iceberg_table.transaction.return_value.__enter__.return_value.set_properties.assert_called_once_with(
        properties_new,
    )
