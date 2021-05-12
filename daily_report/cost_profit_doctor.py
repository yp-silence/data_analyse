"""
收入 利润  医生 就诊 复诊 参数统计
"""

import pandas as pd
import click

from utils.db_utils import fetch_data
from utils.logger_utils import get_logger
from utils.time_utils import get_current_date_str

logger = get_logger()


def __get_hospital_ids(is_md=False):
    if is_md:
        """明德机构"""
        hospital_ids = "10000664,10000666"
    else:
        """非明德机构"""
        hospital_ids = """
        10000423, 10000526, 10000459, 10000507, 10000489, 
        10000589, 10000247, 10000440, 10000395, 10000610
        """
    return hospital_ids


def filter_flag_data(file_path):
    df = pd.read_excel(file_path)
    name_filter_flag = df['名称'].str.contains('齿科|宠物|口腔')
    # 常熟  张家港 昆山 吴江
    address_filter_flag = df['地址'].str.contains('太仓|常熟|张家港|昆山|吴江')
    filter_flag = name_filter_flag | address_filter_flag
    df = df[~filter_flag]
    df.to_excel('苏州市-20210413-过滤后.xlsx', index=False)


def doctor_indicator(start_date, end_date, is_md=False, is_doctor=True):
    """
    is_doctor: True 医生  False 标记医生
    接诊人次 复诊人次 人头指标
    """
    hospital_id = __get_hospital_ids(is_md)
    sql = f"""
     SELECT
            t3.hospital_name 诊所名称,
            DATE_FORMAT( t3.create_date, '%Y-%m' ) 日期,
            {"t3.doctor_name 医生姓名 ," if is_doctor else "t3.consultants_name  标记医生姓名 ,"}
            COUNT( t3.registered_ord_id ) 接诊人次,
            COUNT( IF ( t3.registered_type = 2, t3.registered_ord_id, NULL ) ) 复诊人次,
            IFNULL(ROUND( COUNT( IF ( t3.registered_type = 2, t3.registered_ord_id, NULL ) ), 2 ) / COUNT( t3.registered_ord_id ) ,0) 复诊率,
            COUNT( DISTINCT t3.user_id ) 接诊人头,
            round( COUNT( t3.registered_ord_id ) / COUNT( DISTINCT t3.user_id ), 2 ) "人次/人头"
    FROM
     ord.ord_registered t3
    WHERE
     t3.create_date BETWEEN '{start_date}'
     AND '{end_date}'
     AND t3.state = 5
     AND t3.registered_type IN ( 1, 2 )
     AND t3.hospital_id IN ({hospital_id})
    GROUP BY
        t3.hospital_name,
        date_format( t3.create_date, '%Y-%m' ),
        {"t3.doctor_name" if is_doctor else "t3.consultants_name"}
    order by 诊所名称, 日期 desc,复诊率 desc
    """
    df = fetch_data(sql, is_md=is_md)
    logger.info(f'fetch_total:{len(df)}')
    df['复诊率'] = df['复诊率'].astype('float')
    df['复诊率'] = df['复诊率'].map(lambda val: '{:.2%}'.format(val))
    return df


def __format_charge_columns():
    charge_ch_mapping = {
        "registered_fee": "挂号费",
        "registered_cost_fee": "挂号成本",
        "diagnosis_fee": "诊疗费",
        "diagnosis_cost_fee": "诊疗费成本",
        "check_fee": "门诊检验费",
        "check_cost_fee": "门诊检验成本",
        "medical_fee": "治疗费",
        "medical_cost_fee": "治疗成本",
        "material_fee": "耗材",
        "material_cost_fee": "耗材成本",
        "instrument_fee": "器械",
        "instrument_cost_fee": "器械成本",
        "other1_fee": "生长激素",
        "other1_cost_fee": "生长激素成本",
        "other2_fee": "器械1",
        "other2_cost_fee": "器械1成本",
        "other3_fee": "自定义分类3",
        "other3_cost_fee": "自定义分类3成本",
        "other4_fee": "自定义分类4",
        "other4_cost_fee": "自定义分类4成本",
        "exam_in_room": "科室体检费",
        "exam_in_cost_room": "科室体检成本费",
        "exam_check": "体检检验费",
        "exam_cost_check": "体检检验成本",
        "exam_inspect": "体检检查费",
        "exam_cost_inspect": "体检检查成本",
        "west_medicine_fee": "西药处方",
        "west_medicine_cost_fee": "西药处方成本",
        "cp_medicine_fee": "中成药处方",
        "cp_medicine_cost_fee": "中成药处方成本",
        "trad_medicine_fee": "中药处方",
        "trad_medicine_cost_fee": "中药处方成本",
        "decoction_fee": "代煎费",
        "decoction_cost_fee": "代煎费成本",
        "inspect_advice_fee": "门诊检查费",
        "inspect_advice_cost_fee": "门诊检查成本",
        "other_fee": "其他收费",
        "other_cost_fee": "其他收费成本",
        "total_price": "合计",
        "total_cost_price": "总成本"
    }
    return charge_ch_mapping


def income_cost_indicator(start_date, end_date, is_md=False, is_doctor=True):
    """
    成本 利润  收费项 价格指标
    is_doctor: True doctor False: consultants 标记医生
    is_md: 标记是否是明德机构
    """
    hospital_id = __get_hospital_ids(is_md)
    sql = f"""
        select
            hospital.hospital_name '诊所名称',
            {"pay.doctor_name '医生姓名'," if is_doctor else "pay.consultants_name '标记医生姓名',"}
            date_format(pay.registered_create_date,'%Y-%m')  '日期',
            sum(ofd.total_price)   total_price ,
            sum(ofd.total_cost_price)  total_cost_price ,
            sum(case when ofd.subject_type = 1 then ofd.total_price ELSE 0 END) as registered_fee,
            sum(case when ofd.subject_type = 1 then ofd.total_cost_price ELSE 0 END) as registered_cost_fee,
            sum(case when ofd.subject_type = 2 then ofd.total_price ELSE 0 END )as diagnosis_fee,
            sum(case when ofd.subject_type = 2 then ofd.total_cost_price ELSE 0 END )as diagnosis_cost_fee,
            sum(case when ofd.subject_type = 3 then ofd.total_price ELSE 0 END) as check_fee,
            sum(case when ofd.subject_type = 3 then ofd.total_cost_price ELSE 0 END) as check_cost_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 10 then ofd.total_price ELSE 0 END )as medical_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 10 then ofd.total_cost_price ELSE 0 END )as medical_cost_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =4 then ofd.total_price ELSE 0 END )as material_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =4 then ofd.total_cost_price ELSE 0 END )as material_cost_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =5 then ofd.total_price ELSE 0 END )as instrument_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =5 then ofd.total_cost_price ELSE 0 END )as instrument_cost_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =6 then ofd.total_price ELSE 0 END )as other1_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =6 then ofd.total_cost_price ELSE 0 END )as other1_cost_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =7 then ofd.total_price ELSE 0 END )as other2_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =7 then ofd.total_cost_price ELSE 0 END )as other2_cost_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =8 then ofd.total_price ELSE 0 END )as other3_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =8 then ofd.total_cost_price ELSE 0 END )as other3_cost_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =9 then ofd.total_price ELSE 0 END )as other4_fee,
            sum(case when ofd.subject_type = 5 and ofd.subject_id = 12 and ofd.item_type =9 then ofd.total_cost_price ELSE 0 END )as other4_cost_fee,
            sum(case when ofd.subject_type = 30 and ofd.subject_id = 41 then ofd.total_price ELSE 0 END )as exam_in_room,
               sum(case when ofd.subject_type = 30 and ofd.subject_id = 41 then ofd.total_cost_price ELSE 0 END )as exam_in_cost_room,
            sum(case when ofd.subject_type = 30 and ofd.subject_id = 42 then ofd.total_price ELSE 0 END )as exam_check,
               sum(case when ofd.subject_type = 30 and ofd.subject_id = 42 then ofd.total_cost_price ELSE 0 END )as exam_cost_check,
            sum(case when ofd.subject_type = 30 and ofd.subject_id = 43 then ofd.total_price ELSE 0 END )as exam_inspect,
            sum(case when ofd.subject_type = 30 and ofd.subject_id = 43 then ofd.total_cost_price ELSE 0 END )as exam_cost_inspect,
            sum(case when ofd.subject_type = 10 and ofd.subject_id = 15 then ofd.total_price ELSE 0 END )as west_medicine_fee,
            sum(case when ofd.subject_type = 10 and ofd.subject_id = 15 then ofd.total_cost_price ELSE 0 END )as west_medicine_cost_fee,
            sum(case when ofd.subject_type = 10 and ofd.subject_id = 16 then ofd.total_price ELSE 0 ENd )as cp_medicine_fee,
            sum(case when ofd.subject_type = 10 and ofd.subject_id = 16 then ofd.total_cost_price ELSE 0 ENd )as cp_medicine_cost_fee,
            sum(case when ofd.subject_type = 11 and ofd.subject_id = 20 then ofd.total_price ELSE 0 END )as trad_medicine_fee,
            sum(case when ofd.subject_type = 11 and ofd.subject_id = 20 then ofd.total_cost_price ELSE 0 END )as trad_medicine_cost_fee,
            sum(case when ofd.subject_type = 11 and ofd.subject_id = 25 then ofd.total_price ELSE 0 END) as decoction_fee,
            sum(case when ofd.subject_type = 11 and ofd.subject_id = 25 then ofd.total_cost_price ELSE 0 END) as decoction_cost_fee,
            sum(case when ofd.subject_type = 6 and ofd.subject_id = 31 then ofd.total_price ELSE 0 END) as inspect_advice_fee,
            sum(case when ofd.subject_type = 6 and ofd.subject_id = 31 then ofd.total_cost_price ELSE 0 END) as inspect_advice_cost_fee,
            sum(case when ofd.subject_type = 15 then ofd.total_price ELSE 0 END )as other_fee,
            sum(case when ofd.subject_type = 15 then ofd.total_price ELSE 0 END )as other_cost_fee
     from
        ord.ord_fee_detail ofd,ord.ord_pay pay , sec.sec_hospital  hospital
    where
         pay.hospital_id in ({hospital_id})
    and
           ofd.pay_state =2 and pay.pay_order_id = ofd.pay_order_id  and pay.state =1 and pay.buy_type =1
      and ofd.hospital_id = hospital.hospital_id
     and pay.registered_create_date >'{start_date}' and pay.registered_create_date < '{end_date}'
    GROUP BY
        hospital.hospital_name,
        {"pay.doctor_name" if is_doctor else "pay.consultants_name"},
      date_format(pay.registered_create_date,'%Y-%m')
    order by hospital_name,日期 desc;
    """
    df = fetch_data(sql, is_md=is_md)
    required_fields = __format_charge_columns().keys()
    df[list(required_fields)] = df[list(required_fields)].applymap(lambda val: round(val / 100000, 2))
    df.rename(columns=__format_charge_columns(), inplace=True)
    df['毛利润'] = df['合计'] - df['总成本']
    target_fields = ['诊所名称', '医生姓名', '日期', '挂号费', '挂号成本', '治疗费', '治疗成本', '耗材',
                     '耗材成本', '器械', '器械成本', '中药处方', '中药处方成本', '代煎费',
                     '代煎费成本', '西药处方', '西药处方成本', '中成药处方', '中成药处方成本', '门诊检验费', '门诊检验成本',
                     '门诊检查费', '门诊检查成本', '其他收费', '其他收费成本', '诊疗费', '诊疗费成本', '合计', '总成本', '毛利润']
    if not is_doctor:
        target_fields[1] = '标记医生姓名'
    df = df[target_fields]
    return df


def __sub_merge(df_1, df_2, is_doctor=True):
    if is_doctor:
        target_df = df_1.merge(df_2, on=['诊所名称', '医生姓名', '日期'])
    else:
        target_df = df_1.merge(df_2, on=['诊所名称', '标记医生姓名', '日期'])
    target_df['合计'] = target_df['合计'].astype('float')
    target_df['客单价'] = target_df['合计'] / target_df['接诊人次']
    target_df['客单价'] = target_df['客单价'].map(lambda val: round(val, 2))
    target_df.drop('总成本', inplace=True, axis=1)
    return target_df


def merge_data(start_date, end_date, is_doctor=True, to_excel=True):
    df_1 = doctor_indicator(start_date, end_date, is_doctor=is_doctor)
    df_2 = income_cost_indicator(start_date, end_date, is_doctor=is_doctor)
    df_3 = doctor_indicator(start_date, end_date, is_md=True, is_doctor=is_doctor)
    df_4 = income_cost_indicator(start_date, end_date, is_md=True, is_doctor=is_doctor)
    # after_merge_1 = __sub_merge(doctor_df[0], income_df[0], is_doctor=is_doctor)
    # after_merge_2 = __sub_merge(doctor_df[1], income_df[1], is_doctor=is_doctor)
    after_merge_1 = __sub_merge(df_1, df_2, is_doctor=is_doctor)
    after_merge_2 = __sub_merge(df_3, df_4, is_doctor=is_doctor)
    target_df = pd.concat([after_merge_1, after_merge_2])
    if to_excel:
        suffix = '医生' if is_doctor else '标记医生'
        io = pd.ExcelWriter(f'汇总数据-{suffix}.xlsx')
        target_df.to_excel(io, index=False)
        # 列数
        column_count = target_df.shape[1]
        io.sheets['Sheet1'].set_column(0, column_count - 1, 20)
        io.close()
    else:
        return target_df


def get_all_data(start_date, end_date):
    sheet1_df = merge_data(start_date, end_date, is_doctor=True, to_excel=False)
    sheet2_df = merge_data(start_date, end_date, is_doctor=False, to_excel=False)
    file_name = get_current_date_str() + '汇总数据.xlsx'
    io = pd.ExcelWriter(file_name)
    sheet1_df.to_excel(io, index=False, sheet_name='医生汇总数据')
    sheet2_df.to_excel(io, index=False, sheet_name='标记医生汇总数据')
    io.sheets['医生汇总数据'].set_column(0, sheet1_df.shape[1] - 1, 20)
    io.sheets['标记医生汇总数据'].set_column(0, sheet2_df.shape[1] - 1, 20)
    io.close()
    logger.info(f'{file_name}生成成功！')


@click.command()
@click.option('--start_date', default='2021-01-01', help='开始日期')
@click.option('--end_date', default='2021-04-01', help='结束日期')
@click.option('--is_doctor', default=False, help='医生,标记医生标志')
def run(start_date, end_date, is_doctor):
    merge_data(start_date, end_date, is_doctor=is_doctor)


@click.command()
@click.option('--start_date', default='2021-01-01', help='开始日期')
@click.option('--end_date', default='2021-04-01', help='结束日期')
def generate_all_date(start_date, end_date):
    get_all_data(start_date, end_date)


if __name__ == '__main__':
    generate_all_date()