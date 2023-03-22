//
// Created by jinhai on 23-3-8.
//

#pragma once

#include "common/column_vector/column_vector.h"
#include "common/types/value.h"
#include "main/logger.h"
#include "main/stats/global_resource_usage.h"
#include "main/session.h"
#include "planner/logical_planner.h"
#include "planner/optimizer.h"
#include "executor/physical_planner.h"
#include "scheduler/operator_pipeline.h"
#include "main/infinity.h"

#include "main/profiler/show_logical_plan.h"
#include "parser/sql_parser.h"

namespace infinity {

class SQLRunner {

public:
    static String
    Run(const String& sql_text, bool print = true);

};

}

