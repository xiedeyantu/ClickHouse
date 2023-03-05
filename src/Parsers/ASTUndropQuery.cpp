#include <Parsers/ASTUndropQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

String ASTUndropQuery::getID(char delim) const
{
    return "UndropQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTUndropQuery::clone() const
{
    auto res = std::make_shared<ASTUndropQuery>(*this);
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTUndropQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    settings.ostr << "UNDROP ";
    settings.ostr << "TABLE ";
    settings.ostr << (settings.hilite ? hilite_none : "");

    if (!table && database)
        settings.ostr << backQuoteIfNeed(getDatabase());
    else
        settings.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

    formatOnCluster(settings);
}

}
