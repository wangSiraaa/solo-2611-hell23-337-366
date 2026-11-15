# Restricted MERGE AST v1
原始planner.py仅演示delta加法，是旧接口。升级接口读取merge-plan.json及以下AST。
Plan = {plan_id,source:[SourceRow],matched:[Branch],not_matched:[Branch]}
SourceRow = {source_id:nonempty-text,key:text|null,delta:int64,new_key?:text|null,parent_id?:int64|null}
source_id在一计划内唯一；省略new_key代表不请求改key，不与显式NULL混同。
Branch = {when:BoolExpr,action:"update"|"delete"|"insert",set?:{TargetColumn:ValueExpr}}
matched仅update/delete；not_matched仅insert；按分支顺序取首个when为TRUE的分支，没有TRUE则no-op。
ValueExpr = {literal:int64|text|null} | {source:SourceField} | {target:TargetColumn} | {op:"+"|"-",args:[ValueExpr,ValueExpr]}
BoolExpr = {literal:true|false|null} | {op:"="|"<>"|"<"|">"|"<="|">=",args:[ValueExpr,ValueExpr]} | {op:"and"|"or",args:[BoolExpr,BoolExpr]} | {op:"not",args:[BoolExpr]} | {op:"is_null"|"is_not_null",args:[ValueExpr]}
not_matched引用target为非法AST；整数算术遇NULL为NULL，非整数输入/溢出拒绝整计划；比较与布尔操作采用SQL三值逻辑，文本SQLite BINARY。
TargetColumn可引用id/business_key/amount/parent_id/rev，但set只允许business_key/amount/parent_id；id为系统分配身份，rev由执行入口维护。
insert必须提供business_key、amount，parent_id缺省NULL；update未set列保持旧值；表达式都对冻结语句前快照求值。
同target多source匹配即冲突，不以某条分支最后no-op规避；所有待插入/改名后的唯一键和FK由原子执行整体核验。
