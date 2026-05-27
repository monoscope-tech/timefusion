use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::Expr;

fn extract_placeholder_cast_types(plan: &LogicalPlan) -> Result<HashMap<String, Option<DataType>>> {
    let mut placeholder_types = HashMap::new();
    let mut casted_placeholders = HashSet::new();

    plan.apply(|node| {
        for expr in node.expressions() {
            let _ = expr.apply(|e| {
                if let Expr::Cast(cast) = e {
                    if let Expr::Placeholder(ph) = &*cast.expr {
                        placeholder_types.insert(ph.id.clone(), Some(cast.data_type.clone()));
                        casted_placeholders.insert(ph.id.clone());
                    }
                }

                if let Expr::Placeholder(ph) = e {
                    if !casted_placeholders.contains(&ph.id)
                        && !placeholder_types.contains_key(&ph.id)
                    {
                        placeholder_types.insert(ph.id.clone(), None);
                    }
                }

                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(placeholder_types)
}

pub fn get_inferred_parameter_types(
    plan: &LogicalPlan,
) -> Result<HashMap<String, Option<DataType>>> {
    let param_types = plan.get_parameter_types()?;

    let has_none = param_types.values().any(|v| v.is_none());

    if !has_none {
        Ok(param_types)
    } else {
        let cast_types = extract_placeholder_cast_types(plan)?;

        let mut merged = param_types;

        for (id, opt_type) in cast_types {
            merged
                .entry(id)
                .and_modify(|existing| {
                    if existing.is_none() {
                        *existing = opt_type.clone();
                    }
                })
                .or_insert(opt_type);
        }

        Ok(merged)
    }
}
