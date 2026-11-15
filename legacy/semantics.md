读取真实SQLite目录，只支持题定schema中的每表单列TEXT PK=id，外键单列指向id；允许自引用环。支持ON DELETE CASCADE、SET NULL、NO ACTION，所有FK均DEFERRABLE INITIALLY DEFERRED。未知动作/表/列、坏根或源库已有孤立FK均拒绝计划。NULL外键没有引用；PK和值按SQLite BINARY精确匹配。
plan只读源库，用请求roots形成删除集合：CASCADE从被删父扩展到引用它的子，直到不再增大，多路径/环不重复。再对幸存子处理SET NULL：其父被删则该列置NULL；被删子无需再置NULL。对NO ACTION只检查幸存子是否仍引用被删父，同批已删除该子不算冲突。SET NULL打到NOT NULL列是阻断；受影响的同列赋值只可能NULL，多原因保留全部。输出delete_rows、null_updates、blockers与每动作的外键/根原因，数字不代替实际主键集合。
apply绑定源文件SHA256、请求及规范schema摘要，先复核仍一致，再复制源为新库，使用一笔事务执行请求根删除，允许SQLite自动执行真实外键动作；提交前PRAGMA foreign_key_check、逐表差量和计划预测必须一致，阻断或失败不发布成功产物。plan不得借助临时库真正删除后的结果冒充静态推演；实际执行与独立复制库可作验证oracle。源始终只读，不支持原位线上更新、并发、崩溃恢复或任意触发器；目录存在触发器即拒绝。
NO ACTION为延期事务末校验，不实现RESTRICT的逐步立即检查。本题没有业务保留政策、脱敏、跨副本清理或最小删除优化。最多10表/1000行，根顺序不改变最终计划。
