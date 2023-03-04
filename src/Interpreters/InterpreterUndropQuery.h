#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTUndropQuery.h>

namespace DB
{

class Context;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;
class AccessRightsElements;


class InterpreterUndropQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterUndropQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    /// Undrop table.
    BlockIO execute() override;

    static void executeUnDropQuery(ContextPtr global_context, ContextPtr current_context, const StorageID & target_table_id);

private:
    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    ASTPtr query_ptr;

    BlockIO executeToTable(ASTUndropQuery & query);
};
}
