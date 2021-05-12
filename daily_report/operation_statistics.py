"""
运营统计
"""
import sys
import pandas as pd
import click

sys.path.append('../')

from utils.db_utils import fetch_data
from utils.time_utils import *

logger = get_logger()

PAY_TYPE_MAPPING = {
    1: "现金",
    2: "支付宝",
    3: "微信",
    4: "银行卡",
    5: "会员账号",
    6: "其它",
    7: "乐刷支付",
    8: "商保",
    9: "挂账",
    10: "市民卡"
}


def __format_pay_type(val):
    """支付方式"""
    if val in PAY_TYPE_MAPPING:
        return PAY_TYPE_MAPPING[val]
    else:
        return '###'


def __check_group_type(group_type):
    support_group_fields = ('门诊', '科室', '医生')
    if group_type not in support_group_fields:
        logger.error(f'group fields : {group_type} is invalid, only support {support_group_fields}')
        return
    else:
        pass


def fetch_operate_data_charge_way(start_date, end_date, group_type, time_dimension='day', to_excel=False):
    """
    按照收费方式统计: 医保 现金 支付宝 微信 银行卡 挂账 市民卡
    支持诊所  科室  医生维度
    :param time_dimension: 时间维度
    :param to_excel: 是否生成excel文件
    :param start_date:
    :param end_date:
    :param group_type: 分组条件
    :return:
    """
    __check_group_type(group_type)
    if group_type == "门诊":
        if time_dimension == 'day':
            sql = f"""
                SELECT
                    reg.hospital_name 诊所名称,
                    round( sum( original_amount )/ 10000, 2 ) 原价,
                    round( sum( receivable_amount )/ 10000, 2 ) 应收,
                    round( sum( pay_amount )/ 10000, 2 ) 实收,
                    round( sum( deposit_amount )/ 10000, 2 ) 折扣优惠,
                    round( sum( medical_fund_payment )/ 10000, 2 ) 医保,
                    DATE_FORMAT( ord.create_date, '%Y-%m-%d' ) 付款日期,
                    ord.pay_type 付款方式 
                FROM
                    ord.ord_pay ord
                    INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                    AND ord.buy_type = 1 
                WHERE
                    ord.buy_type = 1 
                    AND ord.state IN ( 1, 5 ) 
                    AND ord.create_date BETWEEN '{start_date}'  AND '{end_date}'
                GROUP BY
                    reg.hospital_name,
                    ord.pay_type,
                    DATE_FORMAT(
                    ord.create_date,
                    '%Y-%m-%d')
            """
            group_filed = ['诊所名称', '付款日期']
        else:
            sql = f"""      
            SELECT
                reg.hospital_name 诊所名称,
                round( sum( original_amount )/ 10000, 2 ) 原价,
                round( sum( receivable_amount )/ 10000, 2 ) 应收,
                round( sum( pay_amount )/ 10000, 2 ) 实收,
                round( sum( deposit_amount ) / 10000, 2 ) 折扣优惠,
                round( sum( medical_fund_payment ) / 10000, 2 ) 医保,
                ord.pay_type 付款方式 
            FROM
                ord.ord_pay ord
                INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                AND ord.buy_type = 1 
            WHERE
                ord.buy_type = 1 
                AND ord.state IN ( 1, 5 ) 
                AND ord.create_date BETWEEN '{start_date}' 
                AND '{end_date}' 
            GROUP BY
                reg.hospital_name,
                ord.pay_type
        """
            group_filed = ['诊所名称']
    elif group_type == '科室':
        if time_dimension == 'day':
            sql = f"""
                    SELECT
                        reg.hospital_name 诊所名称,
                        reg.depart_name 科室,
                        round( sum( original_amount )/ 10000, 2 ) 原价,
                        round( sum( receivable_amount )/ 10000, 2 ) 应收,
                        round( sum( pay_amount )/ 10000, 2 ) 实收,
                        round( sum( deposit_amount ) / 10000, 2 ) 折扣优惠,
                        round( sum( medical_fund_payment ) / 10000, 2 ) 医保,
                        DATE_FORMAT( ord.create_date, '%Y-%m-%d' ) 付款日期,
                        ord.pay_type 付款方式 
                    FROM
                        ord.ord_pay ord
                        INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                        AND ord.buy_type = 1 
                    WHERE
                        ord.buy_type = 1 
                        AND ord.state IN ( 1, 5 ) 
                        AND ord.create_date BETWEEN '{start_date}' and '{end_date}' 
                    GROUP BY
                    reg.hospital_name,
                    reg.depart_name,
                    ord.pay_type,
                    DATE_FORMAT( ord.create_date, '%Y-%m-%d' )
                """
            group_filed = ['诊所名称', '科室', '付款日期']
        else:
            sql = f"""
               SELECT
                    reg.hospital_name 诊所名称,
                    reg.depart_name 科室,
                    round( sum( original_amount )/ 10000, 2 ) 原价,
                    round( sum( receivable_amount )/ 10000, 2 ) 应收,
                    round( sum( pay_amount )/ 10000, 2 ) 实收,
                    round( sum( deposit_amount ) / 10000, 2 ) 折扣优惠,
                    round( sum( medical_fund_payment ) / 10000, 2 ) 医保,
                    ord.pay_type 付款方式 
                FROM
                    ord.ord_pay ord
                    INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                    AND ord.buy_type = 1 
                WHERE
                    ord.buy_type = 1 
                    AND ord.state IN ( 1, 5 ) 
                    AND ord.create_date BETWEEN '{start_date}' 
                    AND '{end_date}' 
                GROUP BY
                    reg.hospital_name,
                    reg.depart_name,
                    ord.pay_type
            """
            group_filed = ['诊所名称', '科室']
    else:
        #  医院下的 医生维度
        if time_dimension == 'day':
            sql = f"""
                SELECT
                    reg.hospital_name 诊所名称,
                    reg.doctor_name 医生,
                    round( sum( original_amount )/ 10000, 2 ) 原价,
                    round( sum( receivable_amount )/ 10000, 2 ) 应收,
                    round( sum( pay_amount )/ 10000, 2 ) 实收,
                    round( sum( deposit_amount )/ 10000, 2 ) 折扣优惠,
                    round( sum( medical_fund_payment )/ 10000, 2 ) 医保,
                    DATE_FORMAT( ord.create_date, '%Y-%m-%d' ) 付款日期,
                    ord.pay_type 付款方式 
                FROM
                    ord.ord_pay ord
                    INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                    AND ord.buy_type = 1 
                WHERE
                    ord.buy_type = 1 
                    AND ord.state IN ( 1, 5 ) 
                    AND ord.create_date BETWEEN '{start_date}' and '{end_date}'
                GROUP BY
                    reg.hospital_name,
                    reg.doctor_name,
                    DATE_FORMAT( ord.create_date, '%Y-%m-%d' ),
                    ord.pay_type
                """
            group_filed = ['诊所名称', '医生', '付款日期']
        else:
            sql = f"""
                SELECT
                    reg.hospital_name 诊所名称,
                    reg.doctor_name 医生,
                    round( sum( original_amount )/ 10000, 2 ) 原价,
                    round( sum( receivable_amount )/ 10000, 2 ) 应收,
                    round( sum( pay_amount )/ 10000, 2 ) 实收,
                    round( sum( deposit_amount ) / 10000, 2 ) 折扣优惠,
                    round( sum( medical_fund_payment ) / 10000, 2 ) 医保,
                    ord.pay_type 付款方式 
                FROM
                    ord.ord_pay ord
                    INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                    AND ord.buy_type = 1 
                WHERE
                    ord.buy_type = 1 
                    AND ord.state IN ( 1, 5 ) 
                    AND ord.create_date BETWEEN '{start_date}'  AND '{end_date}' 
                GROUP BY
                    reg.hospital_name,
                    reg.doctor_name,
                    ord.pay_type 
                ORDER BY  实收 DESC
          """
            group_filed = ['诊所名称', '医生']
    data_df = fetch_data(sql)
    logger.info(f'total find {len(data_df)}条记录')
    data_df['付款方式'] = data_df['付款方式'].map(__format_pay_type)
    other_pay_way = list(PAY_TYPE_MAPPING.values())
    data_df = pd.concat([data_df, pd.DataFrame(columns=other_pay_way)])
    # key 指定的列 索引
    data_df.fillna({key: 0 for key in other_pay_way}, inplace=True)
    for key in other_pay_way:
        pt_flag = data_df['付款方式'] == key
        # 付款金额与实收一致
        data_df.loc[pt_flag, key] = data_df.loc[pt_flag, '实收']
    data_df.drop(labels='付款方式', axis=1, inplace=True)
    logg.info(f'group fields:{group_filed}')
    data_df = data_df.groupby(group_filed).sum().reset_index()
    logger.info(f'total find {len(data_df)}条记录')
    data_df['开始时间'] = start_date
    data_df['结束时间'] = end_date
    logger.info(f'{data_df.head()}')
    if to_excel:
        file_name = f'{group_type}-{time_dimension}-支付方式统计数据.xlsx'
        io = pd.ExcelWriter(file_name)
        data_df.to_excel(io, index=False)
        # 设置单元格宽度
        io.sheets['Sheet1'].set_column('A:S', 20)
        io.close()


def fetch_operate_data_charge_classify(start_date, end_date, group_type, time_dimension='day', to_excel=False):
    """
    按照收费项目统计
    支持的维度: 支持诊所  科室  医生维度
    :param start_date:
    :param end_date:
    :param group_type:
    :param time_dimension:
    :param to_excel:
    :return:
    """
    __check_group_type(group_type)
    if group_type == '门诊':
        if time_dimension == 'day':
            sql = f"""
                    SELECT
                        reg.hospital_name 诊所名称,
                        DATE_FORMAT( ord.create_date, '%Y-%m-%d' ) 付款日期,
                        round( sum( classify.registered_fee )/ 10000, 2 ) 挂号费,
                        round( sum( medical_fee )/ 10000, 2 ) 治疗,
                        round( sum( material_fee )/ 10000, 2 ) 耗材,
                        round( sum( instrument_fee )/ 10000, 2 ) 器械,
                        round( sum( trad_medicine_fee )/ 10000, 2 ) 中药处方,
                        round( sum( decoction_fee )/ 10000, 2 ) 代煎费,
                        round( sum( west_medicine_fee )/ 10000, 2 ) 西药处方,
                        round( sum( cp_medicine_fee )/ 10000, 2 ) 中成药处方,
                        round( sum( check_fee )/ 10000, 2 )门诊检验费,
                        round( sum( inspect_advice_fee )/ 10000, 2 ) 门诊检查费,
                        round( sum( other1_fee )/ 10000, 2 ) 生长激素,
                        round( sum( other2_fee )/ 10000, 2 ) 器械1,
                        round( sum( other3_fee )/ 10000, 2 ) 自定义分类3,
                        round( sum( other4_fee )/ 10000, 2 )自定义分类4,
                        round( sum( exam_in_room )/ 10000, 2 ) 科室体检费,
                        round( sum( exam_check )/ 10000, 2 ) 体检检验费,
                        round( sum( exam_inspect )/ 10000, 2 ) 体检检查费,
                        round( sum( other_fee )/ 10000, 2 ) 其他收费,
                        round( sum( diagnosis_fee )/ 10000, 2 ) 诊疗费,
                        round( sum( classify.receivable_amount )/ 10000, 2 ) 折后总额 
                    FROM
                        ord.ord_pay_classify classify
                        INNER JOIN ord.ord_pay ord ON classify.pay_order_id = ord.pay_order_id
                        INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                    WHERE
                        ord.buy_type = 1 
                        AND ord.state IN ( 1, 5 ) 
                        AND ord.create_date BETWEEN '{start_date}' 
                        AND '{end_date}' 
                    GROUP BY
                        reg.hospital_name,
                        DATE_FORMAT( ord.create_date, '%Y-%m-%d' ) 
                    ORDER BY
                        诊所名称,付款日期 DESC
            """
        else:
            # 诊所维度
            sql = f"""
            SELECT
                reg.hospital_name 诊所名称,
                round( sum( classify.registered_fee )/ 10000, 2 ) 挂号费,
                round( sum( medical_fee )/ 10000, 2 ) 治疗,
                round( sum( material_fee )/ 10000, 2 ) 耗材,
                round( sum( instrument_fee )/ 10000, 2 ) 器械,
                round( sum( trad_medicine_fee )/ 10000, 2 ) 中药处方,
                round( sum( decoction_fee )/ 10000, 2 ) 代煎费,
                round( sum( west_medicine_fee )/ 10000, 2 ) 西药处方,
                round( sum( cp_medicine_fee )/ 10000, 2 ) 中成药处方,
                round( sum( check_fee )/ 10000, 2 )门诊检验费,
                round( sum( inspect_advice_fee )/ 10000, 2 ) 门诊检查费,
                round( sum( other1_fee )/ 10000, 2 ) 生长激素,
                round( sum( other2_fee )/ 10000, 2 ) 器械1,
                round( sum( other3_fee )/ 10000, 2 ) 自定义分类3,
                round( sum( other4_fee )/ 10000, 2 )自定义分类4,
                round( sum( exam_in_room )/ 10000, 2 ) 科室体检费,
                round( sum( exam_check )/ 10000, 2 ) 体检检验费,
                round( sum( exam_inspect )/ 10000, 2 ) 体检检查费,
                round( sum( other_fee )/ 10000, 2 ) 其他收费,
                round( sum( diagnosis_fee )/ 10000, 2 ) 诊疗费,
                round( sum( classify.receivable_amount )/ 10000, 2 ) 折后总额 
            FROM
                ord.ord_pay_classify classify
                INNER JOIN ord.ord_pay ord ON classify.pay_order_id = ord.pay_order_id
                INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
            WHERE
                ord.buy_type = 1 
                AND ord.state IN ( 1, 5 ) 
                AND ord.create_date BETWEEN '{start_date}' 
                AND '{end_date}' 
            GROUP BY
                reg.hospital_name 
            ORDER BY 诊所名称
        """
    elif group_type == '科室':
        if time_dimension == 'day':
            sql = f"""
                SELECT
                    reg.hospital_name 诊所名称,
                    reg.depart_name 科室,
                    DATE_FORMAT( ord.create_date, '%Y-%m-%d' ) 付款日期,
                    round( sum( classify.registered_fee )/ 10000, 2 ) 挂号费,
                    round( sum( medical_fee )/ 10000, 2 ) 治疗,
                    round( sum( material_fee )/ 10000, 2 ) 耗材,
                    round( sum( instrument_fee )/ 10000, 2 ) 器械,
                    round( sum( trad_medicine_fee )/ 10000, 2 ) 中药处方,
                    round( sum( decoction_fee )/ 10000, 2 ) 代煎费,
                    round( sum( west_medicine_fee )/ 10000, 2 ) 西药处方,
                    round( sum( cp_medicine_fee )/ 10000, 2 ) 中成药处方,
                    round( sum( check_fee )/ 10000, 2 )门诊检验费,
                    round( sum( inspect_advice_fee )/ 10000, 2 ) 门诊检查费,
                    round( sum( other1_fee )/ 10000, 2 ) 生长激素,
                    round( sum( other2_fee )/ 10000, 2 ) 器械1,
                    round( sum( other3_fee )/ 10000, 2 ) 自定义分类3,
                    round( sum( other4_fee )/ 10000, 2 )自定义分类4,
                    round( sum( exam_in_room )/ 10000, 2 ) 科室体检费,
                    round( sum( exam_check )/ 10000, 2 ) 体检检验费,
                    round( sum( exam_inspect )/ 10000, 2 ) 体检检查费,
                    round( sum( other_fee )/ 10000, 2 ) 其他收费,
                    round( sum( diagnosis_fee )/ 10000, 2 ) 诊疗费,
                    round( sum( classify.receivable_amount )/ 10000, 2 ) 折后总额 
                FROM
                    ord.ord_pay_classify classify
                    INNER JOIN ord.ord_pay ord ON classify.pay_order_id = ord.pay_order_id
                    INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                WHERE
                    ord.buy_type = 1 
                    AND ord.state IN ( 1, 5 ) 
                    AND ord.create_date BETWEEN '{start_date}'  AND '{end_date}'
                GROUP BY
                    reg.hospital_name,
                    reg.depart_name,
                    DATE_FORMAT(
                    ord.create_date,
                    '%Y-%m-%d')
                    order by 诊所名称 , 付款日期 desc
                """
        else:
            sql = f"""
                SELECT
                    reg.hospital_name  诊所名称,
                    reg.depart_name  科室,
                    round( sum( classify.registered_fee ) / 10000, 2 ) 挂号费,
                    round( sum( medical_fee ) / 10000, 2 ) 治疗,
                    round( sum( material_fee ) / 10000, 2 ) 耗材,
                    round( sum( instrument_fee ) / 10000, 2 ) 器械,
                    round( sum( trad_medicine_fee ) / 10000, 2 ) 中药处方,
                    round( sum( decoction_fee ) / 10000, 2 ) 代煎费,
                    round( sum( west_medicine_fee ) / 10000, 2 ) 西药处方,
                    round( sum( cp_medicine_fee ) / 10000, 2 ) 中成药处方,
                    round( sum( check_fee )/ 10000, 2 )门诊检验费,
                    round( sum( inspect_advice_fee ) / 10000, 2 ) 门诊检查费,
                    round( sum( other1_fee ) / 10000, 2 ) 生长激素,
                    round( sum( other2_fee ) / 10000, 2 ) 器械1,
                    round( sum( other3_fee ) / 10000, 2 ) 自定义分类3,
                    round( sum( other4_fee ) / 10000, 2 )自定义分类4,
                    round( sum( exam_in_room ) / 10000, 2 ) 科室体检费,
                    round( sum( exam_check ) / 10000, 2 ) 体检检验费,
                    round( sum( exam_inspect ) / 10000, 2 ) 体检检查费,
                    round( sum( other_fee ) / 10000, 2 ) 其他收费,
                    round( sum( diagnosis_fee ) / 10000, 2 ) 诊疗费,
                    round( sum( classify.receivable_amount ) / 10000, 2 ) 折后总额
            FROM
                ord.ord_pay_classify classify
            INNER JOIN ord.ord_pay ord ON classify.pay_order_id = ord.pay_order_id
            INNER JOIN ord.ord_registered  reg ON reg.registered_ord_id = ord.order_id 
            WHERE
                ord.buy_type = 1 
                AND ord.state IN ( 1, 5 ) 
                AND ord.create_date BETWEEN '{start_date}' 
                AND '{end_date}'
            GROUP BY  
            reg.hospital_name,
            reg.depart_name
                """
    else:
        # 医生维度
        if time_dimension == 'day':
            sql = f"""
                SELECT
                    reg.hospital_name 诊所名称,
                    reg.doctor_name 医生,
                    DATE_FORMAT( ord.create_date, '%Y-%m-%d' ) 付款日期,
                    round( sum( classify.registered_fee )/ 10000, 2 ) 挂号费,
                    round( sum( medical_fee )/ 10000, 2 ) 治疗,
                    round( sum( material_fee )/ 10000, 2 ) 耗材,
                    round( sum( instrument_fee )/ 10000, 2 ) 器械,
                    round( sum( trad_medicine_fee )/ 10000, 2 ) 中药处方,
                    round( sum( decoction_fee )/ 10000, 2 ) 代煎费,
                    round( sum( west_medicine_fee )/ 10000, 2 ) 西药处方,
                    round( sum( cp_medicine_fee )/ 10000, 2 ) 中成药处方,
                    round( sum( check_fee )/ 10000, 2 )门诊检验费,
                    round( sum( inspect_advice_fee )/ 10000, 2 ) 门诊检查费,
                    round( sum( other1_fee )/ 10000, 2 ) 生长激素,
                    round( sum( other2_fee )/ 10000, 2 ) 器械1,
                    round( sum( other3_fee )/ 10000, 2 ) 自定义分类3,
                    round( sum( other4_fee )/ 10000, 2 )自定义分类4,
                    round( sum( exam_in_room )/ 10000, 2 ) 科室体检费,
                    round( sum( exam_check )/ 10000, 2 ) 体检检验费,
                    round( sum( exam_inspect )/ 10000, 2 ) 体检检查费,
                    round( sum( other_fee )/ 10000, 2 ) 其他收费,
                    round( sum( diagnosis_fee )/ 10000, 2 ) 诊疗费,
                    round( sum( classify.receivable_amount )/ 10000, 2 ) 折后总额 
                FROM
                    ord.ord_pay_classify classify
                    INNER JOIN ord.ord_pay ord ON classify.pay_order_id = ord.pay_order_id
                    INNER JOIN ord.ord_registered reg ON reg.registered_ord_id = ord.order_id 
                WHERE
                    ord.buy_type = 1 
                    AND ord.state IN ( 1, 5 ) 
                    AND ord.create_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY
                    reg.hospital_name,
                    reg.doctor_name,
                    DATE_FORMAT( ord.create_date, '%Y-%m-%d' ) 
                ORDER BY  诊所名称,付款日期 DESC
            """
        else:
            sql = f"""
            SELECT
                reg.hospital_name  诊所名称,
                reg.doctor_name  医生,
                round( sum( classify.registered_fee ) / 10000, 2 ) 挂号费,
                round( sum( medical_fee ) / 10000, 2 ) 治疗,
                round( sum( material_fee ) / 10000, 2 ) 耗材,
                round( sum( instrument_fee ) / 10000, 2 ) 器械,
                round( sum( trad_medicine_fee ) / 10000, 2 ) 中药处方,
                round( sum( decoction_fee ) / 10000, 2 ) 代煎费,
                round( sum( west_medicine_fee ) / 10000, 2 ) 西药处方,
                round( sum( cp_medicine_fee ) / 10000, 2 ) 中成药处方,
                round( sum( check_fee )/ 10000, 2 )门诊检验费,
                round( sum( inspect_advice_fee ) / 10000, 2 ) 门诊检查费,
                round( sum( other1_fee ) / 10000, 2 ) 生长激素,
                round( sum( other2_fee ) / 10000, 2 ) 器械1,
                round( sum( other3_fee ) / 10000, 2 ) 自定义分类3,
                round( sum( other4_fee ) / 10000, 2 )自定义分类4,
                round( sum( exam_in_room ) / 10000, 2 ) 科室体检费,
                round( sum( exam_check ) / 10000, 2 ) 体检检验费,
                round( sum( exam_inspect ) / 10000, 2 ) 体检检查费,
                round( sum( other_fee ) / 10000, 2 ) 其他收费,
                round( sum( diagnosis_fee ) / 10000, 2 ) 诊疗费,
                round( sum( classify.receivable_amount ) / 10000, 2 ) 折后总额
        FROM
            ord.ord_pay_classify classify
        INNER JOIN ord.ord_pay ord ON classify.pay_order_id = ord.pay_order_id
        INNER JOIN ord.ord_registered  reg ON reg.registered_ord_id = ord.order_id 
        WHERE
            ord.buy_type = 1 
            AND ord.state IN ( 1, 5 ) 
            AND ord.create_date BETWEEN '{start_date}' 
            AND '{end_date}'
        GROUP BY  
        reg.hospital_name,
        reg.doctor_name
        """
    logg.info(f'query sql:{sql}')
    data_df = fetch_data(sql)
    logg.info(f'total fetch:{len(data_df)}条记录')
    if to_excel:
        file_name = f'{group_type}-{time_dimension}-费项目统计数据.xlsx'
        io = pd.ExcelWriter(file_name)
        data_df['开始时间'] = start_date
        data_df['结束时间'] = end_date
        data_df.to_excel(io, index=False)
        io.sheets['Sheet1'].set_column('A:X', 20)
        io.close()


def __calc_statistics_range(dimension):
    if dimension == 'day':
        # 默认为月初到当前日期的 统计区间
        tmp = get_current_date_str()
        start = tmp[0:8] + '01 00:00:00'
        end = tmp + " 23:59:59"
        return start, end
    elif dimension == 'week':
        # 自然周 从本周一 到 现在日期
        start, end = get_week_day(current=True)
        start += ' 00:00:00'
        end += " 23:59:59"
    elif dimension == 'month':
        start = get_begin_of_month()
        end = get_current_date_str()
        start += ' 00:00:00'
        end += " 23:59:59"
    else:
        # year
        start = get_current_date_str()[:5] + '01-01 00:00:00'
        end = get_current_date_str() + ' 23:59:59'
    return start, end


@click.command()
@click.option('--time_dimension', default='year', help='时间维度', type=click.Choice(['day', 'week', 'month', 'year']))
@click.option('--to_excel', default=True, help='是否生成excel')
@click.option('--group_type', default='医生', help='分组维度')
@click.option('--show_type', default='2', type=click.Choice(['1', '2']), help='展示方式')
def run(time_dimension, group_type, show_type, to_excel):
    start, end = __calc_statistics_range(time_dimension)
    if show_type == '1':
        # 按照收费方式统计指标
        fetch_operate_data_charge_way(start, end, group_type, time_dimension, to_excel)
    else:
        # 按照 收费项目统计指标
        fetch_operate_data_charge_classify(start, end, group_type, time_dimension, to_excel)


def test_group():
    df = pd.read_excel('运营数据.xlsx')
    other_pay_way = ["现金", "支付宝", "微信", "银行卡", "会员账号", "其它", "乐刷支付", "商保", "挂账", "市民卡"]
    df = pd.concat([df, pd.DataFrame(columns=other_pay_way)])
    df.fillna(0, inplace=True)
    for key in other_pay_way:
        pt_flag = df['付款方式'] == key
        df.loc[pt_flag, key] = df.loc[pt_flag, '实收']
    df.drop(labels='付款方式', axis=1, inplace=True)
    df = df.groupby('诊所名称').sum().reset_index()
    print(df.head())
    df.to_excel('test.xlsx', index=False)


if __name__ == '__main__':
    run()
