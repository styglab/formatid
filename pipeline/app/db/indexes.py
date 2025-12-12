from sqlalchemy import text
#
#
CREATE_IDX_ATTACHMENT_STATUS_CREATED = text("""
CREATE INDEX IF NOT EXISTS idx_attachment_status_created
ON bid_notice_attachment (status, created_at);
""")

CREATE_IDX_ATTACHMENT_NOTICE = text("""
CREATE INDEX IF NOT EXISTS idx_attachment_notice
ON bid_notice_attachment (bid_ntce_no, bid_ntce_ord);
""")

