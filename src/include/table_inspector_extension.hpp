#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

class TableInspectorExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
