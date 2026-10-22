#include "table_inspector_functions.hpp"

#include "inspect_block_usage.hpp"
#include "inspect_column.hpp"
#include "inspect_database.hpp"
#include "inspect_storage.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

namespace {

FunctionDescription DescribeFunction(vector<LogicalType> parameter_types, vector<string> parameter_names,
                                     string description, vector<string> examples, vector<string> categories) {
	FunctionDescription result;
	result.parameter_types = std::move(parameter_types);
	result.parameter_names = std::move(parameter_names);
	result.description = std::move(description);
	result.examples = std::move(examples);
	result.categories = std::move(categories);
	return result;
}

// Expose documented names through duckdb_functions() without changing positional-only binding semantics.
void SetParameterNames(TableFunction &function, const FunctionDescription &description) {
	auto &signature = function.GetSignature();
	D_ASSERT(signature.GetParameterCount() == description.parameter_names.size());
	for (idx_t i = 0; i < description.parameter_names.size(); i++) {
		signature.GetParameter(i).SetName(Identifier(description.parameter_names[i]));
	}
}

void RegisterTableFunction(ExtensionLoader &loader, TableFunction function, FunctionDescription description) {
	SetParameterNames(function, description);
	CreateTableFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(std::move(description));
	loader.RegisterFunction(std::move(info));
}

void RegisterTableFunction(ExtensionLoader &loader, TableFunctionSet functions,
                           vector<FunctionDescription> descriptions) {
	functions.ApplyToFunctions([&](TableFunction &function) {
		for (const auto &description : descriptions) {
			const FunctionSignature described_signature(description.parameter_types, LogicalType::INVALID);
			if (function.GetSignature().IsSameOverload(described_signature)) {
				SetParameterNames(function, description);
				return;
			}
		}
		D_ASSERT(false); // Every registered overload must have matching documentation.
	});
	CreateTableFunctionInfo info(std::move(functions));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions = std::move(descriptions);
	loader.RegisterFunction(std::move(info));
}

} // namespace

void RegisterTableInspectorFunctions(ExtensionLoader &loader) {
	RegisterTableFunction(
	    loader, GetInspectStorageFunction(),
	    DescribeFunction(
	        /*parameter_types=*/ {}, /*parameter_names=*/ {},
	        /*description=*/
	        "Returns persistent attached databases with their DuckDB file and write-ahead log sizes in bytes.",
	        /*examples=*/ {"SELECT * FROM inspect_storage();"}, /*categories=*/ {"storage", "inspection"}));

	RegisterTableFunction(
	    loader, GetInspectDatabaseFunction(),
	    {DescribeFunction(
	         /*parameter_types=*/ {LogicalType::VARCHAR},
	         /*parameter_names=*/ {"database_name"},
	         /*description=*/"Returns per-table persisted data and index sizes for the specified persistent database.",
	         /*examples=*/ {"SELECT * FROM inspect_database('my_database');"},
	         /*categories=*/ {"storage", "inspection"}),
	     DescribeFunction(
	         /*parameter_types=*/ {}, /*parameter_names=*/ {},
	         /*description=*/"Returns per-table persisted data and index sizes for the current persistent database.",
	         /*examples=*/ {"SELECT * FROM inspect_database();"},
	         /*categories=*/ {"storage", "inspection"})});

	RegisterTableFunction(
	    loader, GetInspectColumnFunction(),
	    {DescribeFunction(/*parameter_types=*/
	                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                      /*parameter_names=*/ {"database_name", "table_name", "column_name"},
	                      /*description=*/
	                      "Returns per-segment compression and size details for a column in the specified database.",
	                      /*examples=*/ {"SELECT * FROM inspect_column('my_database', 'my_table', 'my_column');"},
	                      /*categories=*/ {"storage", "inspection"}),
	     DescribeFunction(
	         /*parameter_types=*/ {LogicalType::VARCHAR, LogicalType::VARCHAR},
	         /*parameter_names=*/ {"table_name", "column_name"},
	         /*description=*/"Returns per-segment compression and size details for a column in the current database.",
	         /*examples=*/ {"SELECT * FROM inspect_column('my_table', 'my_column');"},
	         /*categories=*/ {"storage", "inspection"})});

	RegisterTableFunction(
	    loader, GetInspectBlockUsageFunction(),
	    {DescribeFunction(
	         /*parameter_types=*/ {LogicalType::VARCHAR},
	         /*parameter_names=*/ {"database_name"},
	         /*description=*/"Returns the storage breakdown by component for the specified persistent database.",
	         /*examples=*/ {"SELECT * FROM inspect_block_usage('my_database');"},
	         /*categories=*/ {"storage", "inspection"}),
	     DescribeFunction(
	         /*parameter_types=*/ {}, /*parameter_names=*/ {},
	         /*description=*/"Returns the storage breakdown by component for the current persistent database.",
	         /*examples=*/ {"SELECT * FROM inspect_block_usage();"},
	         /*categories=*/ {"storage", "inspection"})});
}

} // namespace duckdb
