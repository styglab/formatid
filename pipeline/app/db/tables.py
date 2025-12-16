from sqlalchemy import (
    Table, Column, String, Text, Integer, BigInteger, Boolean, TIMESTAMP, JSON, MetaData
)
#
#
metadata = MetaData()

bid_notice = Table(
    "bid_notice",
    metadata,
    Column("bid_ntce_no", Text, primary_key=True), # bidNtceNo
    Column("bid_ntce_ord", Text, primary_key=True), # bidNtceOrd
    Column("bid_ntce_ord_num", Integer, nullable=False), # bidNtceOrd
    
    Column("bid_type", String(20), nullable=False), # 물품: thng, 공사: cnst
    Column("bid_ntce_nm", Text, nullable=False), # bidNtceNm
    Column("ntce_kind_nm", Text), # ntceKindNm
    Column("bid_ntce_dt", TIMESTAMP(timezone=True)), # bidNtceDt
    
    Column("ntce_instt_cd", Text), # ntceInsttCd    
    Column("ntce_instt_nm", Text), # ntceInsttNm      
    Column("dminstt_cd", Text), # dminsttCd    
    Column("dminstt_nm", Text), # dminsttNm    
    
    # 물품 전용
    Column("dtil_prdct_clsfc_no", Text), # dtilPrdctClsfcNo            
    Column("dtil_prdct_clsfc_no_nm", Text), # dtilPrdctClsfcNoNm
    
    # 일정
    Column("bid_begin_dt", TIMESTAMP(timezone=True)), # bidBeginDt  
    Column("bid_clse_dt", TIMESTAMP(timezone=True)), # bidClseDt  
    Column("openg_dt", TIMESTAMP(timezone=True)), # opengDt 
    
    # 금액 
    Column("bdgt_amt", BigInteger, nullable=True), # 예산: 물품: asignBdgtAmt / 공사: BdgtAmt
    Column("presmpt_prce", BigInteger, nullable=True), # 추정가격: presmptPrce    
    
    # 메타
    Column("raw_json", JSON, nullable=False),
    Column("is_latest", Boolean, default=False),
    Column("created_at", TIMESTAMP(timezone=True)),
    Column("updated_at", TIMESTAMP(timezone=True)),
)

bid_notice_attachment = Table(
    "bid_notice_attachment",
    metadata,
    Column("id", BigInteger, primary_key=True),

    Column("bid_ntce_no", Text, nullable=False),
    Column("bid_ntce_ord", Text, nullable=False),
    Column("file_seq", Integer, nullable=False),

    Column("file_name", Text),
    Column("download_url", Text, nullable=False),

    Column("storage_path", Text),
    Column("file_size", BigInteger),
    Column("file_hash", Text),

    Column("status", Text, nullable=False, default="pending"),
    Column("attempts", Integer, nullable=False, default=0),
    Column("last_error", Text),

    Column("started_at", TIMESTAMP(timezone=True)),
    Column("finished_at", TIMESTAMP(timezone=True)),
    Column("created_at", TIMESTAMP(timezone=True)),
    Column("updated_at", TIMESTAMP(timezone=True)),
)


