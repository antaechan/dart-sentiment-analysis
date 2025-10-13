"""
DART 공시 유형별 스키마 정의
각 공시 유형의 필드 구성을 선언적으로 정의
"""

# 필드 타입 정의
DISCLOSURE_SCHEMAS = {
    "유상증자 결정": {
        "excluded_fields": {
            "rcept_no",
            "corp_cls",
            "corp_code",
            "corp_name",
            "ic_mthn",
            "ssl_at",
        },
        "sections": [
            {
                "title": "유상증자 발행정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "법인구분", "key": "corp_cls"},
                    {"label": "이사회결의일", "key": "bddd"},
                ],
            },
            {
                "title": "신주 종류 및 수",
                "fields": [
                    {
                        "label": "  보통주식(주)",
                        "key": "nstk_ostk_cnt",
                        "format": "shares",
                    },
                    {
                        "label": "  기타주식(주)",
                        "key": "nstk_estk_cnt",
                        "format": "shares",
                    },
                    {"label": "1주당 액면가액(원)", "key": "fv_ps", "format": "amount"},
                ],
            },
            {
                "title": "증자전 발행주식총수",
                "fields": [
                    {
                        "label": "  보통주식(주)",
                        "key": "bfic_tisstk_ostk",
                        "format": "shares",
                    },
                    {
                        "label": "  기타주식(주)",
                        "key": "bfic_tisstk_estk",
                        "format": "shares",
                    },
                ],
            },
            {
                "title": "자금조달의 목적",
                "fields": [
                    {"label": "  시설자금(원)", "key": "fdpp_fclt", "format": "amount"},
                    {
                        "label": "  영업양수자금(원)",
                        "key": "fdpp_bsninh",
                        "format": "amount",
                    },
                    {"label": "  운영자금(원)", "key": "fdpp_op", "format": "amount"},
                    {
                        "label": "  채무상환자금(원)",
                        "key": "fdpp_dtrp",
                        "format": "amount",
                    },
                    {
                        "label": "  타법인증권 취득자금(원)",
                        "key": "fdpp_ocsa",
                        "format": "amount",
                    },
                    {"label": "  기타자금(원)", "key": "fdpp_etc", "format": "amount"},
                ],
            },
            {
                "title": "발행정보",
                "fields": [
                    {"label": "증자방식", "key": "ic_mthn"},
                    {"label": "공매도해당여부", "key": "ssl_at"},
                    {"label": "공매도 시작일", "key": "ssl_bgd"},
                    {"label": "공매도 종료일", "key": "ssl_edd"},
                ],
            },
        ],
    },
    "무상증자 결정": {
        "excluded_fields": {
            "rcept_no",
            "corp_cls",
            "corp_code",
            "corp_name",
            "ic_mthn",
            "ssl_at",
        },
        "sections": [
            {
                "title": "무상증자 발행정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "법인구분", "key": "corp_cls"},
                    {"label": "이사회결의일(결정일)", "key": "bddd"},
                ],
            },
            {
                "title": "신주 종류 및 수",
                "fields": [
                    {
                        "label": "  보통주식(주)",
                        "key": "nstk_ostk_cnt",
                        "format": "shares",
                    },
                    {
                        "label": "  기타주식(주)",
                        "key": "nstk_estk_cnt",
                        "format": "shares",
                    },
                    {"label": "1주당 액면가액(원)", "key": "fv_ps", "format": "amount"},
                ],
            },
            {
                "title": "증자전 발행주식총수",
                "fields": [
                    {
                        "label": "  보통주식(주)",
                        "key": "bfic_tisstk_ostk",
                        "format": "shares",
                    },
                    {
                        "label": "  기타주식(주)",
                        "key": "bfic_tisstk_estk",
                        "format": "shares",
                    },
                ],
            },
            {
                "title": "신주배정기준일 및 배정정보",
                "fields": [
                    {"label": "신주배정기준일", "key": "nstk_asstd"},
                    {
                        "label": "1주당 신주배정 주식수(보통주식)",
                        "key": "nstk_ascnt_ps_ostk",
                    },
                    {
                        "label": "1주당 신주배정 주식수(기타주식)",
                        "key": "nstk_ascnt_ps_estk",
                    },
                    {"label": "신주의 배당기산일", "key": "nstk_dividrk"},
                    {"label": "신주권교부예정일", "key": "nstk_dlprd"},
                    {"label": "신주의 상장 예정일", "key": "nstk_lstprd"},
                ],
            },
            {
                "title": "이사회/감사 정보",
                "fields": [
                    {"label": "사외이사 참석(명)", "key": "od_a_at_t"},
                    {"label": "사외이사 불참(명)", "key": "od_a_at_b"},
                    {"label": "감사(감사위원)참석 여부", "key": "adt_a_atn"},
                ],
            },
        ],
    },
    "전환사채권 발행결정": {
        "excluded_fields": {
            "rcept_no",
            "corp_cls",
            "corp_code",
            "corp_name",
            "ftc_stt_atn",
            "bdis_mthn",
            "rs_sm_atn",
        },
        "sections": [
            {
                "title": "전환사채 발행정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "이사회결의일", "key": "bddd"},
                ],
            },
            {
                "title": "발행정보",
                "fields": [
                    {"label": "사채종류", "key": "bd_knd"},
                    {"label": "발행금액", "key": "bd_fta", "format": "amount"},
                    {"label": "발행방법", "key": "bdis_mthn"},
                    {"label": "사채만기일", "key": "bd_mtd"},
                ],
            },
            {
                "title": "이자율 정보",
                "fields": [
                    {"label": "표면이자율", "key": "bd_intr_ex", "format": "percent"},
                    {"label": "만기이자율", "key": "bd_intr_sf", "format": "percent"},
                ],
            },
            {
                "title": "전환 정보",
                "fields": [
                    {
                        "label": "전환가액",
                        "key": "cv_prc",
                        "format": "amount",
                        "suffix": " (1주당)",
                    },
                    {"label": "전환비율", "key": "cv_rt", "format": "percent"},
                    {
                        "label": "주식총수 대비",
                        "key": "cvisstk_tisstk_vs",
                        "format": "percent",
                    },
                    {
                        "label": "전환청구기간",
                        "key": ["cvrqpd_bgd", "cvrqpd_edd"],
                        "template": "{} ~ {}",
                    },
                    {
                        "label": "최저조정가액",
                        "key": "act_mktprcfl_cvprc_lwtrsprc",
                        "format": "amount",
                    },
                ],
            },
            {
                "title": "일정",
                "fields": [
                    {"label": "청약일", "key": "sbd"},
                    {"label": "납입일", "key": "pymd"},
                ],
            },
            {
                "title": "자금조달 목적",
                "optional": True,
                "fields": [
                    {"label": "시설자금", "key": "fdpp_fclt", "format": "amount"},
                    {"label": "운영자금", "key": "fdpp_op", "format": "amount"},
                    {"label": "채무상환자금", "key": "fdpp_dtrp", "format": "amount"},
                    {
                        "label": "타법인증권취득자금",
                        "key": "fdpp_ocsa",
                        "format": "amount",
                    },
                    {"label": "기타자금", "key": "fdpp_etc", "format": "amount"},
                ],
            },
            {
                "title": "기타정보",
                "fields": [
                    {"label": "대표주관회사", "key": "rpmcmp", "optional": True},
                    {"label": "보증기관", "key": "grint", "optional": True},
                    {"label": "증권신고서 제출대상", "key": "rs_sm_atn"},
                    {"label": "제출면제사유", "key": "ex_sm_r", "optional": True},
                ],
            },
        ],
    },
    "교환사채권 발행결정": {
        "excluded_fields": {"rcept_no", "corp_cls", "corp_code", "corp_name"},
        "sections": [
            {
                "title": "교환사채 발행정보",
                "separator": True,
                "fields": [
                    {"label": "공시대상회사명", "key": "corp_name"},
                    {"label": "사채의 종류(회차)", "key": "bd_tm"},
                    {"label": "사채의 종류(종류)", "key": "bd_knd"},
                    {
                        "label": "사채의 권면(전자등록)총액 (원)",
                        "key": "bd_fta",
                        "format": "amount",
                    },
                    {
                        "label": "해외발행(권면(전자등록)총액)",
                        "key": "ovis_fta",
                        "format": "amount",
                    },
                    {
                        "label": "해외발행(권면(전자등록)총액(통화단위))",
                        "key": "ovis_fta_crn",
                    },
                    {"label": "해외발행(기준환율등)", "key": "ovis_ster"},
                    {"label": "해외발행(발행지역)", "key": "ovis_isar"},
                    {"label": "해외발행(해외상장시 시장의 명칭)", "key": "ovis_mktnm"},
                ],
            },
            {
                "title": "자금조달의 목적",
                "fields": [
                    {"label": "시설자금 (원)", "key": "fdpp_fclt", "format": "amount"},
                    {
                        "label": "영업양수자금 (원)",
                        "key": "fdpp_bsninh",
                        "format": "amount",
                    },
                    {"label": "운영자금 (원)", "key": "fdpp_op", "format": "amount"},
                    {
                        "label": "채무상환자금 (원)",
                        "key": "fdpp_dtrp",
                        "format": "amount",
                    },
                    {
                        "label": "타법인 증권 취득자금 (원)",
                        "key": "fdpp_ocsa",
                        "format": "amount",
                    },
                    {"label": "기타자금 (원)", "key": "fdpp_etc", "format": "amount"},
                ],
            },
            {
                "title": "이자율 정보",
                "fields": [
                    {
                        "label": "사채의 이율(표면이자율 (%))",
                        "key": "bd_intr_ex",
                        "format": "percent",
                    },
                    {
                        "label": "사채의 이율(만기이자율 (%))",
                        "key": "bd_intr_sf",
                        "format": "percent",
                    },
                ],
            },
            {
                "title": None,  # 섹션 제목 없음
                "fields": [
                    {"label": "사채만기일", "key": "bd_mtd"},
                    {"label": "사채발행방법", "key": "bdis_mthn"},
                ],
            },
            {
                "title": "교환에 관한 사항",
                "fields": [
                    {"label": "교환비율 (%)", "key": "ex_rt", "format": "percent"},
                    {"label": "교환가액 (원/주)", "key": "ex_prc", "format": "amount"},
                    {"label": "교환가액 결정방법", "key": "ex_prc_dmth"},
                    {"label": "교환대상(종류)", "key": "extg"},
                    {
                        "label": "교환대상(주식수)",
                        "key": "extg_stkcnt",
                        "format": "amount",
                    },
                    {
                        "label": "교환대상(주식총수 대비 비율(%))",
                        "key": "extg_tisstk_vs",
                        "format": "percent",
                    },
                    {"label": "교환청구기간(시작일)", "key": "exrqpd_bgd"},
                    {"label": "교환청구기간(종료일)", "key": "exrqpd_edd"},
                ],
            },
            {
                "title": "일정",
                "fields": [
                    {"label": "청약일", "key": "sbd"},
                    {"label": "납입일", "key": "pymd"},
                ],
            },
            {
                "title": None,
                "fields": [
                    {"label": "대표주관회사", "key": "rpmcmp"},
                    {"label": "보증기관", "key": "grint"},
                    {"label": "이사회결의일(결정일)", "key": "bddd"},
                    {
                        "label": "사외이사 참석여부(참석 (명))",
                        "key": "od_a_at_t",
                        "format": "amount",
                    },
                    {
                        "label": "사외이사 참석여부(불참 (명))",
                        "key": "od_a_at_b",
                        "format": "amount",
                    },
                    {"label": "감사(감사위원) 참석여부", "key": "adt_a_atn"},
                    {"label": "증권신고서 제출대상 여부", "key": "rs_sm_atn"},
                    {"label": "제출을 면제받은 경우 그 사유", "key": "ex_sm_r"},
                    {
                        "label": "당해 사채의 해외발행과 연계된 대차거래 내역",
                        "key": "ovis_ltdtl",
                    },
                    {"label": "공정거래위원회 신고대상 여부", "key": "ftc_stt_atn"},
                ],
            },
        ],
    },
    "감자 결정": {
        "excluded_fields": {
            "rcept_no",
            "corp_cls",
            "corp_code",
            "corp_name",
            "ftc_stt_atn",
        },
        "sections": [
            {
                "title": "감자 결정 정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "법인구분", "key": "corp_cls"},
                    {"label": "이사회결의일(결정일)", "key": "bddd"},
                ],
            },
            {
                "title": "감자주식 종류 및 수",
                "fields": [
                    {
                        "label": "  보통주식(주)",
                        "key": "crstk_ostk_cnt",
                        "format": "shares",
                    },
                    {
                        "label": "  기타주식(주)",
                        "key": "crstk_estk_cnt",
                        "format": "shares",
                    },
                ],
            },
            {
                "title": "감자 정보",
                "fields": [
                    {"label": "감자방법", "key": "cr_mth"},
                    {"label": "감자사유", "key": "cr_rs"},
                    {"label": "감자기준일", "key": "cr_std"},
                    {"label": "1주당 액면가액(원)", "key": "fv_ps", "format": "amount"},
                ],
            },
            {
                "title": "감자비율",
                "fields": [
                    {
                        "label": "  보통주식(%)",
                        "key": "cr_rt_ostk",
                        "format": "percent",
                    },
                    {
                        "label": "  기타주식(%)",
                        "key": "cr_rt_estk",
                        "format": "percent",
                    },
                ],
            },
            {
                "title": "발행주식총수 (감자전/감자후)",
                "fields": [
                    {
                        "label": "  감자전 - 보통주식(주)",
                        "key": "bfcr_tisstk_ostk",
                        "format": "shares",
                    },
                    {
                        "label": "  감자전 - 기타주식(주)",
                        "key": "bfcr_tisstk_estk",
                        "format": "shares",
                    },
                    {
                        "label": "  감자후 - 보통주식(주)",
                        "key": "atcr_tisstk_ostk",
                        "format": "shares",
                    },
                    {
                        "label": "  감자후 - 기타주식(주)",
                        "key": "atcr_tisstk_estk",
                        "format": "shares",
                    },
                ],
            },
            {
                "title": "자본금 변동",
                "fields": [
                    {
                        "label": "감자전 자본금(원)",
                        "key": "bfcr_cpt",
                        "format": "amount",
                    },
                    {
                        "label": "감자후 자본금(원)",
                        "key": "atcr_cpt",
                        "format": "amount",
                    },
                ],
            },
            {
                "title": "감자일정",
                "fields": [
                    {"label": "주주총회 예정일", "key": "crsc_gmtsck_prd"},
                    {"label": "명의개서정지기간", "key": "crsc_trnmsppd"},
                    {"label": "구주권 제출기간", "key": "crsc_osprpd"},
                    {"label": "매매거래 정지예정기간", "key": "crsc_trspprpd"},
                    {"label": "구주권 제출기간(시작일)", "key": "crsc_osprpd_bgd"},
                    {"label": "구주권 제출기간(종료일)", "key": "crsc_osprpd_edd"},
                    {
                        "label": "매매거래 정지예정기간(시작일)",
                        "key": "crsc_trspprpd_bgd",
                    },
                    {
                        "label": "매매거래 정지예정기간(종료일)",
                        "key": "crsc_trspprpd_edd",
                    },
                    {"label": "신주권교부예정일", "key": "crsc_nstkdlprd"},
                    {"label": "신주상장예정일", "key": "crsc_nstklstprd"},
                ],
            },
            {
                "title": "채권자 이의제출기간",
                "fields": [
                    {"label": "  시작일", "key": "cdobprpd_bgd"},
                    {"label": "  종료일", "key": "cdobprpd_edd"},
                    {"label": "구주권/신주권 교부장소", "key": "ospr_nstkdl_pl"},
                ],
            },
            {
                "title": "기타 정보",
                "fields": [
                    {"label": "사외이사 참석(명)", "key": "od_a_at_t"},
                    {"label": "사외이사 불참(명)", "key": "od_a_at_b"},
                    {"label": "감사(감사위원) 참석여부", "key": "adt_a_atn"},
                    {"label": "공정거래위원회 신고대상 여부", "key": "ftc_stt_atn"},
                ],
            },
        ],
    },
    "자기주식취득 신탁계약 체결 결정": {
        "excluded_fields": {"rcept_no", "corp_cls", "corp_code", "corp_name"},
        "sections": [
            {
                "title": "자기주식취득 신탁계약 체결정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "이사회결의일", "key": "bddd"},
                ],
            },
            {
                "title": "계약 정보",
                "fields": [
                    {"label": "계약금액", "key": "ctr_prc", "format": "amount"},
                    {
                        "label": "계약기간",
                        "key": ["ctr_pd_bgd", "ctr_pd_edd"],
                        "template": "{} ~ {}",
                    },
                    {"label": "계약목적", "key": "ctr_pp"},
                    {"label": "계약체결기관", "key": "ctr_cns_int"},
                    {"label": "계약체결 예정일자", "key": "ctr_cns_prd"},
                    {"label": "위탁투자중개업자", "key": "cs_iv_bk"},
                ],
            },
            {
                "title": "계약 전 자기주식 보유현황",
                "subsections": [
                    {
                        "title": "[배당가능범위 내 취득]",
                        "fields": [
                            {
                                "label": "  보통주식",
                                "key": ["aq_wtn_div_ostk", "aq_wtn_div_ostk_rt"],
                                "format": ["shares", "percent"],
                                "template": "{} ({})",
                            },
                            {
                                "label": "  기타주식",
                                "key": ["aq_wtn_div_estk", "aq_wtn_div_estk_rt"],
                                "format": ["shares", "percent"],
                                "template": "{} ({})",
                            },
                        ],
                    },
                    {
                        "title": "[기타취득]",
                        "fields": [
                            {
                                "label": "  보통주식",
                                "key": ["eaq_ostk", "eaq_ostk_rt"],
                                "format": ["shares", "percent"],
                                "template": "{} ({})",
                            },
                            {
                                "label": "  기타주식",
                                "key": ["eaq_estk", "eaq_estk_rt"],
                                "format": ["shares", "percent"],
                                "template": "{} ({})",
                            },
                        ],
                    },
                ],
            },
            {
                "title": "이사회 정보",
                "fields": [
                    {
                        "label": "사외이사 참석",
                        "key": ["od_a_at_t", "od_a_at_b"],
                        "template": "{}명 / 불참: {}명",
                    },
                    {"label": "감사(위원) 참석여부", "key": "adt_a_atn"},
                ],
            },
        ],
    },
    "자기주식취득 신탁계약 해지 결정": {
        "excluded_fields": {"rcept_no", "corp_cls", "corp_code", "corp_name"},
        "sections": [
            {
                "title": "자기주식취득 신탁계약 해지 결정 정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "이사회결의일(결정일)", "key": "bddd"},
                ],
            },
            {
                "title": "계약 정보",
                "fields": [
                    {
                        "label": "해지 전 계약금액",
                        "key": "ctr_prc_bfcc",
                        "format": "amount",
                    },
                    {
                        "label": "해지 후 계약금액",
                        "key": "ctr_prc_atcc",
                        "format": "amount",
                    },
                    {
                        "label": "해지 전 계약기간",
                        "key": ["ctr_pd_bfcc_bgd", "ctr_pd_bfcc_edd"],
                        "template": "{} ~ {}",
                    },
                    {"label": "해지목적", "key": "cc_pp"},
                    {"label": "해지기관", "key": "cc_int"},
                    {"label": "해지예정일자", "key": "cc_prd"},
                    {"label": "해지후 신탁재산의 반환방법", "key": "tp_rm_atcc"},
                ],
            },
            {
                "title": "해지 전 자기주식 보유현황",
                "subsections": [
                    {
                        "title": "[배당가능범위 내 취득]",
                        "fields": [
                            {
                                "label": "  보통주식",
                                "key": ["aq_wtn_div_ostk", "aq_wtn_div_ostk_rt"],
                                "format": ["shares", "percent"],
                                "template": "{} ({})",
                            },
                            {
                                "label": "  기타주식",
                                "key": ["aq_wtn_div_estk", "aq_wtn_div_estk_rt"],
                                "format": ["shares", "percent"],
                                "template": "{} ({})",
                            },
                        ],
                    },
                    {
                        "title": "[기타취득]",
                        "fields": [
                            {
                                "label": "  보통주식",
                                "key": ["eaq_ostk", "eaq_ostk_rt"],
                                "format": ["shares", "percent"],
                                "template": "{} ({})",
                            },
                            {
                                "label": "  기타주식",
                                "key": ["eaq_estk", "eaq_estk_rt"],
                                "format": ["shares", "percent"],
                                "template": "{} ({})",
                            },
                        ],
                    },
                ],
            },
            {
                "title": "이사회 정보",
                "fields": [
                    {
                        "label": "사외이사 참석",
                        "key": ["od_a_at_t", "od_a_at_b"],
                        "template": "{}명 / 불참: {}명",
                    },
                    {"label": "감사(위원) 참석여부", "key": "adt_a_atn"},
                ],
            },
        ],
    },
    "영업정지": {
        "excluded_fields": {"rcept_no", "corp_cls", "corp_code", "corp_name"},
        "sections": [
            {
                "title": "영업정지 정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "법인구분", "key": "corp_cls"},
                ],
            },
            {
                "title": None,
                "fields": [
                    {"label": "영업정지 분야", "key": "bsnsp_rm"},
                    {"label": "영업정지 내역(금액)", "key": "bsnsp_amt"},
                    {"label": "영업정지 내역(최근매출총액)", "key": "rsl"},
                    {"label": "영업정지 내역(매출액 대비)", "key": "sl_vs"},
                    {"label": "영업정지 내역(대규모법인여부)", "key": "ls_atn"},
                    {
                        "label": "영업정지 내역(거래소 의무공시 해당 여부)",
                        "key": "krx_stt_atn",
                    },
                ],
            },
            {
                "title": "영업정지 상세",
                "fields": [
                    {"label": "영업정지 내용", "key": "bsnsp_cn"},
                    {"label": "영업정지 사유", "key": "bsnsp_rs"},
                    {"label": "향후대책", "key": "ft_ctp"},
                    {"label": "영업정지영향", "key": "bsnsp_af"},
                ],
            },
            {
                "title": "일정 및 이사회",
                "fields": [
                    {"label": "영업정지일자", "key": "bsnspd"},
                    {"label": "이사회결의일(결정일)", "key": "bddd"},
                    {"label": "사외이사 참석여부(참석)", "key": "od_a_at_t"},
                    {"label": "사외이사 참석여부(불참)", "key": "od_a_at_b"},
                    {"label": "감사(감사위원) 참석여부", "key": "adt_a_atn"},
                ],
            },
        ],
    },
    "회생절차 개시신청": {
        "excluded_fields": {"rcept_no", "corp_cls", "corp_code", "corp_name"},
        "sections": [
            {
                "title": "회생절차 개시신청 정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "법인구분", "key": "corp_cls"},
                ],
            },
            {
                "title": "신청 정보",
                "fields": [
                    {"label": "신청인 (회사와의 관계)", "key": "apcnt"},
                    {"label": "관할법원", "key": "cpct"},
                    {"label": "신청사유", "key": "rq_rs"},
                    {"label": "신청일자", "key": "rqd"},
                ],
            },
            {
                "title": "향후대책 및 일정",
                "fields": [
                    {"label": None, "key": "ft_ctp_sc"},  # 라벨 없이 값만 출력
                ],
            },
        ],
    },
    "소송 등의 제기": {
        "excluded_fields": {"rcept_no", "corp_cls", "corp_code", "corp_name"},
        "sections": [
            {
                "title": "소송 등의 제기 정보",
                "separator": True,
                "fields": [
                    {"label": "회사명", "key": "corp_name"},
                    {"label": "법인구분", "key": "corp_cls"},
                    {"label": "접수번호", "key": "rcept_no"},
                ],
            },
            {
                "title": "소송 기본정보",
                "fields": [
                    {"label": "사건의 명칭", "key": "icnm"},
                    {"label": "원고ㆍ신청인", "key": "ac_ap"},
                    {"label": "청구내용", "key": "rq_cn"},
                ],
            },
            {
                "title": "소송 일정 및 상태",
                "fields": [
                    {"label": "관할법원", "key": "cpct"},
                    {"label": "향후대책", "key": "ft_ctp"},
                    {"label": "제기일자", "key": "lgd"},
                    {"label": "확인일자", "key": "cfd"},
                ],
            },
        ],
    },
}
