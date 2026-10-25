-- 将已有短备注无损扩展为长文本；重复执行不会清空内容。
-- 应在发布支持长篇 Markdown 备注的应用之前执行。
ALTER TABLE stock_info
  MODIFY COLUMN mark_reason MEDIUMTEXT NULL COMMENT '股票备注（Markdown 原文）';
