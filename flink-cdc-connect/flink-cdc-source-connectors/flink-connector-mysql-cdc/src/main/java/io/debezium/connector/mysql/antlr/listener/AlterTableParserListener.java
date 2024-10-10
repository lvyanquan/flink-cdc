/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.debezium.antlr.AntlrDdlParser.getText;

/**
 * Parser listener that is parsing MySQL ALTER TABLE statements.
 *
 * <p>Copied from Debezium project(v1.9.8.Final) to support to parse alter sql with multi events.
 *
 * <p>Changes: Add new fields tempColumnNames and tempColumnOrder to catch the changes in name and
 * order. Finally apply these changes to tableEditor in method exitAlterTable.
 */
public class AlterTableParserListener extends MySqlParserBaseListener {

    private static final int STARTING_INDEX = 1;

    private static final Logger LOG = LoggerFactory.getLogger(AlterTableParserListener.class);

    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;

    private TableEditor tableEditor;
    private ColumnEditor defaultValueColumnEditor;
    private ColumnDefinitionParserListener columnDefinitionListener;
    private DefaultValueParserListener defaultValueListener;

    private List<ColumnEditor> columnEditors;
    private int parsingColumnIndex = STARTING_INDEX;

    /** The timestamp appended after the renamed column name. */
    private long ts;

    /**
     * When a column A is renamed as B in Rename column or Change column, this change will not be
     * applied to tableEditor immediately. A new name `B + timestamp` will be used and recorded in
     * this map. And replace `B + timestamp` to B in method `exitAlterTable`.
     *
     * <p>key: the temp column name (B + timestamp)/value: the original changed name (B)
     *
     * <p>Note: the added method/change is VVR required.
     */
    private Map<String, String> tempColumnNames;

    /**
     * When the order of a column is changed in Modify column or Change column, this change will not
     * be applied to tableEditor immediately. The changes will be recorded in this list and reorder
     * them in method `exitAlterTable` finally.
     *
     * <p>For each element in this list (element): If the array's length is 1, this column
     * (elements[0]) will be reordered as the first column. If the array's length is 2, the first
     * column (element[0]) will be reordered after the second column (element[1]).
     *
     * <p>Note: the added method/change is VVR required.
     */
    private List<String[]> tempColumnOrder;

    public AlterTableParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        final TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        if (parser.databaseTables().forTable(tableId) == null) {
            LOG.debug("Ignoring ALTER TABLE statement for non-captured table {}", tableId);
            return;
        }
        tableEditor = parser.databaseTables().editTable(tableId);
        if (tableEditor == null) {
            throw new ParsingException(
                    null,
                    "Trying to alter table "
                            + tableId.toString()
                            + ", which does not exist. Query: "
                            + getText(ctx));
        }
        tempColumnNames = new HashMap<>();
        tempColumnOrder = new ArrayList<>();
        ts = System.currentTimeMillis();
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        parser.runIfNotNull(
                () -> {
                    listeners.remove(columnDefinitionListener);
                    for (String[] order : tempColumnOrder) {
                        if (order.length != 2) {
                            tableEditor.reorderColumn(order[0], null);
                        } else {
                            String name = order[1];
                            for (Map.Entry<String, String> entry : tempColumnNames.entrySet()) {
                                if (entry.getValue().equals(name)) {
                                    name = entry.getKey();
                                    break;
                                }
                            }
                            tableEditor.reorderColumn(order[0], name);
                        }
                    }
                    for (String col : tempColumnNames.keySet()) {
                        tableEditor.renameColumn(col, tempColumnNames.get(col));
                    }
                    parser.databaseTables().overwriteTable(tableEditor.create());
                    parser.signalAlterTable(tableEditor.tableId(), null, ctx.getParent());
                },
                tableEditor);
        super.exitAlterTable(ctx);
        tableEditor = null;
        tempColumnNames = null;
        tempColumnOrder = null;
    }

    @Override
    public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    String columnName = parser.parseName(ctx.uid(0));
                    ColumnEditor columnEditor = Column.editor().name(columnName);
                    columnDefinitionListener =
                            new ColumnDefinitionParserListener(
                                    tableEditor, columnEditor, parser, listeners);
                    listeners.add(columnDefinitionListener);
                },
                tableEditor);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    tableEditor.addColumn(column);

                    String columnName = column.name();
                    if (ctx.FIRST() != null) {
                        tableEditor.reorderColumn(columnName, null);
                    } else if (ctx.AFTER() != null) {
                        String afterColumn = parser.parseName(ctx.uid(1));
                        tableEditor.reorderColumn(columnName, afterColumn);
                    }
                    listeners.remove(columnDefinitionListener);
                },
                tableEditor,
                columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void enterAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        // multiple columns are added. Initialize a list of column editors for them
        parser.runIfNotNull(
                () -> {
                    columnEditors = new ArrayList<>(ctx.uid().size());
                    for (MySqlParser.UidContext uidContext : ctx.uid()) {
                        String columnName = parser.parseName(uidContext);
                        columnEditors.add(Column.editor().name(columnName));
                    }
                    columnDefinitionListener =
                            new ColumnDefinitionParserListener(
                                    tableEditor, columnEditors.get(0), parser, listeners);
                    listeners.add(columnDefinitionListener);
                },
                tableEditor);
        super.enterAlterByAddColumns(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (columnEditors != null) {
                        // column editor list is not null when a multiple columns are parsed in one
                        // statement
                        if (columnEditors.size() > parsingColumnIndex) {
                            // assign next column editor to parse another column definition
                            columnDefinitionListener.setColumnEditor(
                                    columnEditors.get(parsingColumnIndex++));
                        } else {
                            // all columns parsed
                            // reset global variables for next parsed statement
                            columnEditors.forEach(
                                    columnEditor -> tableEditor.addColumn(columnEditor.create()));
                            columnEditors = null;
                            parsingColumnIndex = STARTING_INDEX;
                        }
                    }
                },
                tableEditor,
                columnEditors);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        parser.runIfNotNull(
                () -> {
                    columnEditors.forEach(
                            columnEditor -> tableEditor.addColumn(columnEditor.create()));
                    listeners.remove(columnDefinitionListener);
                },
                tableEditor,
                columnEditors);
        super.exitAlterByAddColumns(ctx);
    }

    @Override
    public void enterAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    String oldColumnName = parser.parseName(ctx.oldColumn);
                    Column existingColumn = tableEditor.columnWithName(oldColumnName);
                    if (existingColumn != null) {
                        // DBZ-771 unset previously set default value, as it's not kept by MySQL;
                        // for any column modifications a new
                        // default value (which could be the same) has to be provided by the
                        // column_definition which we'll parse later
                        // on; only in 8.0 (not yet supported by this parser) columns can be renamed
                        // without repeating the full column
                        // definition; so in fact it's arguably not correct to use edit() on the
                        // existing column to begin with, but
                        // I'm going to leave this as is for now, to be prepared for the ability of
                        // updating column definitions in 8.0
                        ColumnEditor columnEditor = existingColumn.edit();
                        columnEditor.unsetDefaultValueExpression();

                        columnDefinitionListener =
                                new ColumnDefinitionParserListener(
                                        tableEditor, columnEditor, parser, listeners);
                        listeners.add(columnDefinitionListener);
                    } else {
                        throw new ParsingException(
                                null,
                                "Trying to change column "
                                        + oldColumnName
                                        + " in "
                                        + tableEditor.tableId().toString()
                                        + " table, which does not exist. Query: "
                                        + getText(ctx));
                    }
                },
                tableEditor);
        super.enterAlterByChangeColumn(ctx);
    }

    @Override
    public void exitAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    tableEditor.addColumn(column);
                    String newColumnName = parser.parseName(ctx.newColumn);
                    if (newColumnName != null && !column.name().equalsIgnoreCase(newColumnName)) {
                        String tempNewColumnName = newColumnName + ts;
                        tempColumnNames.put(tempNewColumnName, newColumnName);
                        tableEditor.renameColumn(column.name(), tempNewColumnName);
                        newColumnName = tempNewColumnName;

                        if (ctx.FIRST() != null) {
                            tempColumnOrder.add(new String[] {newColumnName});
                        } else if (ctx.afterColumn != null) {
                            tempColumnOrder.add(
                                    new String[] {
                                        newColumnName, parser.parseName(ctx.afterColumn)
                                    });
                        }
                    } else {
                        if (ctx.FIRST() != null) {
                            tableEditor.reorderColumn(newColumnName, null);
                        } else if (ctx.afterColumn != null) {
                            tableEditor.reorderColumn(
                                    newColumnName, parser.parseName(ctx.afterColumn));
                        }
                    }
                    listeners.remove(columnDefinitionListener);
                },
                tableEditor,
                columnDefinitionListener);
        super.exitAlterByChangeColumn(ctx);
    }

    @Override
    public void enterAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    String columnName = parser.parseName(ctx.uid(0));
                    Column existingColumn = tableEditor.columnWithName(columnName);
                    if (existingColumn != null) {
                        ColumnEditor columnEditor = existingColumn.edit();
                        columnEditor.unsetDefaultValueExpression();

                        columnDefinitionListener =
                                new ColumnDefinitionParserListener(
                                        tableEditor, columnEditor, parser, listeners);
                        listeners.add(columnDefinitionListener);
                    } else {
                        throw new ParsingException(
                                null,
                                "Trying to change column "
                                        + columnName
                                        + " in "
                                        + tableEditor.tableId().toString()
                                        + " table, which does not exist. Query: "
                                        + getText(ctx));
                    }
                },
                tableEditor);
        super.enterAlterByModifyColumn(ctx);
    }

    @Override
    public void exitAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    tableEditor.addColumn(column);

                    if (ctx.FIRST() != null) {
                        tableEditor.reorderColumn(column.name(), null);
                    } else if (ctx.AFTER() != null) {
                        String afterColumn = parser.parseName(ctx.uid(1));
                        tempColumnOrder.add(new String[] {column.name(), afterColumn});
                    }
                    listeners.remove(columnDefinitionListener);
                },
                tableEditor,
                columnDefinitionListener);
        super.exitAlterByModifyColumn(ctx);
    }

    @Override
    public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    tableEditor.removeColumn(parser.parseName(ctx.uid()));
                },
                tableEditor);
        super.enterAlterByDropColumn(ctx);
    }

    @Override
    public void enterAlterByRename(MySqlParser.AlterByRenameContext ctx) {
        parser.runIfNotNull(
                () -> {
                    final TableId newTableId =
                            ctx.uid() != null
                                    ? parser.resolveTableId(
                                            parser.currentSchema(), parser.parseName(ctx.uid()))
                                    : parser.parseQualifiedTableId(ctx.fullId());
                    parser.databaseTables().overwriteTable(tableEditor.create());
                    parser.databaseTables().renameTable(tableEditor.tableId(), newTableId);
                    tableEditor = parser.databaseTables().editTable(newTableId);
                },
                tableEditor);
        super.enterAlterByRename(ctx);
    }

    @Override
    public void enterAlterByChangeDefault(MySqlParser.AlterByChangeDefaultContext ctx) {
        parser.runIfNotNull(
                () -> {
                    String columnName = parser.parseName(ctx.uid());
                    Column column = tableEditor.columnWithName(columnName);
                    if (column != null) {
                        defaultValueColumnEditor = column.edit();
                        if (ctx.SET() != null) {
                            defaultValueListener =
                                    new DefaultValueParserListener(
                                            defaultValueColumnEditor,
                                            new AtomicReference<Boolean>(column.isOptional()));
                            listeners.add(defaultValueListener);
                        } else if (ctx.DROP() != null) {
                            defaultValueColumnEditor.unsetDefaultValueExpression();
                        }
                    }
                },
                tableEditor);
        super.enterAlterByChangeDefault(ctx);
    }

    @Override
    public void exitAlterByChangeDefault(MySqlParser.AlterByChangeDefaultContext ctx) {
        parser.runIfNotNull(
                () -> {
                    tableEditor.updateColumn(defaultValueColumnEditor.create());
                    listeners.remove(defaultValueListener);
                    defaultValueColumnEditor = null;
                },
                defaultValueColumnEditor);
        super.exitAlterByChangeDefault(ctx);
    }

    @Override
    public void enterAlterByAddPrimaryKey(MySqlParser.AlterByAddPrimaryKeyContext ctx) {
        parser.runIfNotNull(
                () -> {
                    parser.parsePrimaryIndexColumnNamesInAlter(
                            ctx.indexColumnNames(), tableEditor, tempColumnNames);
                },
                tableEditor);
        super.enterAlterByAddPrimaryKey(ctx);
    }

    @Override
    public void enterAlterByDropPrimaryKey(MySqlParser.AlterByDropPrimaryKeyContext ctx) {
        parser.runIfNotNull(
                () -> {
                    tableEditor.setPrimaryKeyNames(new ArrayList<>());
                },
                tableEditor);
        super.enterAlterByDropPrimaryKey(ctx);
    }

    @Override
    public void enterAlterByAddUniqueKey(MySqlParser.AlterByAddUniqueKeyContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (!tableEditor.hasPrimaryKey()) {
                        // this may eventually get overwritten by a real PK
                        parser.parsePrimaryIndexColumnNamesInAlter(
                                ctx.indexColumnNames(), tableEditor, tempColumnNames);
                    }
                },
                tableEditor);
        super.enterAlterByAddUniqueKey(ctx);
    }

    @Override
    public void enterAlterByRenameColumn(MySqlParser.AlterByRenameColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    String oldColumnName = parser.parseName(ctx.oldColumn);
                    Column existingColumn = tableEditor.columnWithName(oldColumnName);
                    if (existingColumn != null) {
                        // DBZ-771 unset previously set default value, as it's not kept by MySQL;
                        // for any column modifications a new
                        // default value (which could be the same) has to be provided by the
                        // column_definition which we'll parse later
                        // on; only in 8.0 (not yet supported by this parser) columns can be renamed
                        // without repeating the full column
                        // definition; so in fact it's arguably not correct to use edit() on the
                        // existing column to begin with, but
                        // I'm going to leave this as is for now, to be prepared for the ability of
                        // updating column definitions in 8.0
                        ColumnEditor columnEditor = existingColumn.edit();
                        // columnEditor.unsetDefaultValue();

                        columnDefinitionListener =
                                new ColumnDefinitionParserListener(
                                        tableEditor, columnEditor, parser, listeners);
                        listeners.add(columnDefinitionListener);
                    } else {
                        throw new ParsingException(
                                null,
                                "Trying to change column "
                                        + oldColumnName
                                        + " in "
                                        + tableEditor.tableId().toString()
                                        + " table, which does not exist. Query: "
                                        + getText(ctx));
                    }
                },
                tableEditor);
        super.enterAlterByRenameColumn(ctx);
    }

    @Override
    public void exitAlterByRenameColumn(MySqlParser.AlterByRenameColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    tableEditor.addColumn(column);
                    String newColumnName = parser.parseName(ctx.newColumn);
                    if (newColumnName != null && !column.name().equalsIgnoreCase(newColumnName)) {
                        String tempNewColumnName = newColumnName + ts;
                        tempColumnNames.put(tempNewColumnName, newColumnName);
                        tableEditor.renameColumn(column.name(), tempNewColumnName);
                    }
                    listeners.remove(columnDefinitionListener);
                },
                tableEditor,
                columnDefinitionListener);
        super.exitAlterByRenameColumn(ctx);
    }

    @Override
    public void enterTableOptionComment(MySqlParser.TableOptionCommentContext ctx) {
        if (!parser.skipComments()) {
            parser.runIfNotNull(
                    () -> {
                        if (ctx.COMMENT() != null) {
                            tableEditor.setComment(
                                    parser.withoutQuotes(ctx.STRING_LITERAL().getText()));
                        }
                    },
                    tableEditor);
        }
        super.enterTableOptionComment(ctx);
    }
}
