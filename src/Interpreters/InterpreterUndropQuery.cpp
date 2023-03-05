#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterUndropQuery.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTUndropQuery.h>

#include "config.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
}

InterpreterUndropQuery::InterpreterUndropQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterUndropQuery::execute()
{
    auto & undrop = query_ptr->as<ASTUndropQuery &>();
    if (!undrop.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    if (undrop.table)
        return executeToTable(undrop);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Nothing to undrop, both names are empty");
}

BlockIO InterpreterUndropQuery::executeToTable(ASTUndropQuery & query)
{
    auto table_id = StorageID(query);
    auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);

    auto context = getContext();
    if (table_id.database_name.empty())
        query.setDatabase(table_id.database_name = context->getCurrentDatabase());

    auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->tryGetTable(table_id.table_name, context))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
        "Cannot Undrop table {}, it already exist",
        table_id.getNameForLogs());

    DatabaseCatalog::instance().dequeueDroppedTableCleanup(table_id);
    return {};
}

AccessRightsElements InterpreterUndropQuery::getRequiredAccessForDDLOnCluster() const
{
    AccessRightsElements required_access;
    const auto & undrop = query_ptr->as<const ASTUndropQuery &>();

    required_access.emplace_back(AccessType::UNDROP_TABLE, undrop.getDatabase(), undrop.getTable());
    return required_access;
}
}
