"""
订单相关的日报
"""
import sys
import pandas as pd
import click
import re

sys.path.append('../')

from utils.db_utils import fetch_data
from utils.time_utils import *
from utils.comm_utils import clean_file
from utils.mail_utils import send_attachment_mail

logger = get_logger()


def __calculate_statistics_date(to_str=False):
    current_date = get_current_date_str()
    before_date = date_add(current_date, -1, to_str=to_str)
    start_date = str(before_date.year) + '-' + str(before_date.month).zfill(2) + str('-') + '01'
    return start_date, current_date


def __format_order_state(val):
    state_mapping = {
        10: '待填写患者详情',
        11: '待回复',
        12: '待通话',
        13: '问诊中',
        14: '已完成'
    }
    if val in state_mapping:
        return state_mapping[val]
    else:
        return '###'


def __format_pay_state(val):
    state_mapping = {
        0: '待支付',
        9: '预支付',
        1: '支付完成',
        5: '已退款'
    }
    if val in state_mapping:
        return state_mapping[val]
    else:
        return '###'


def fetch_inquiry_data(start_date, end_date):
    sql = f"""
    select
        inquiry_ord_id 订单编号,
        buy_type 问诊类型,
        user.user_name 患者姓名,
        user.bill_id 患者联系方式,
        doctor_name 问诊医生,
        doctor.phone 医生联系方式,
        doctor.department 科室,
        round(ord_amount / 10000, 2) 问诊金额,
        inquiry.create_date 创建时间,
        ord_state 订单状态,
        pay_state 付款状态,
        abnormal_state 异常状态
    from 
        ord.ord_inquiry inquiry
    inner join 
        sec.sec_app_doctor doctor
    on inquiry.doctor_id = doctor.id
    inner join user.usr_user user
    on inquiry.user_id = user.user_id
    where
        -- 邻家好医机构的订单
        doctor.id IN(SELECT id FROM sec.sec_app_doctor WHERE binding_mechanism = '10000046')
        -- doctor.id NOT IN(SELECT id FROM sec.sec_app_doctor WHERE binding_mechanism = '10001')
        -- and doctor.recommend_id not in (37,39)
        order by inquiry_ord_id desc
    """
    logger.info(f'query sql:{sql}')
    ord_df = fetch_data(sql)
    logger.info(f'total {len(ord_df)}条记录')
    order_columns_origin = ord_df.columns.values.tolist()
    file_name = '问诊订单.xlsx'
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    if len(ord_df) > 0:
        ord_df['问诊类型'] = ord_df['问诊类型'].map(lambda val: '图文问诊' if val == 10 else '电话问诊')
        ord_df['订单状态'] = ord_df['订单状态'].map(__format_order_state)
        ord_df['付款状态'] = ord_df['付款状态'].map(__format_pay_state)
        ord_df['异常状态'] = ord_df['异常状态'].map(lambda val: '正常' if val == 0 else '异常')
        logger.info(ord_df)
        start_date_time = start_date + ' 00:00:00'
        end_date_time = end_date + ' 00:00:00'
        logger.info(f'start_date:{start_date_time}')
        logger.info(f'end_date:{end_date_time}')
        ord_df['创建时间'] = ord_df['创建时间'].astype('str')
        f1 = ord_df['创建时间'] > start_date_time
        f2 = ord_df['创建时间'] <= end_date_time
        ord_df = ord_df[f1 & f2]
        logger.info(f'after filter:{len(ord_df)} 记录')
        ord_df.to_excel(writer, sheet_name='问诊订单原始数据', index=False)
        statistics_df = ord_df.groupby(['问诊医生', '患者姓名', '问诊类型', '付款状态']).agg({'问诊金额': 'sum'}).reset_index()
        statistics_df.rename(columns={'问诊金额': '总金额'}, inplace=True)
        statistics_df.sort_values(by=['总金额'], ascending=False, inplace=True)
        statistics_df.to_excel(writer, sheet_name='汇总信息', index=False)
        writer.sheets['问诊订单原始数据'].set_column("A:L", 20)
        writer.sheets['汇总信息'].set_column('A:E', 20)
        writer.close()
    else:
        logger.warning(f'统计区间内未发现数据,将生成空的数据')
        order_columns_statistics = ['问诊医生', '问诊类型', '付款状态', '总金额']
        sheet_one_df = pd.DataFrame(columns=order_columns_origin)
        sheet_two_df = pd.DataFrame(columns=order_columns_statistics)
        sheet_one_df.to_excel(writer, index=False, sheet_name='问诊订单原始数据')
        sheet_two_df.to_excel(writer, index=False, sheet_name='汇总信息')
        writer.close()
    return file_name


@click.command()
@click.option('--end_date', default='2021-04-01', help='结束日期')
@click.option('--start_date', default='2021-03-01', help='开始日期')
@click.option('--send_mail', default=False, help='是否发送邮件')
def run(start_date, end_date, send_mail):
    if start_date == '' and end_date == '':
        start_date, end_date = __calculate_statistics_date()
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    compile = re.compile(pattern)
    f1 = bool(compile.match(start_date))
    f2 = bool(compile.match(end_date))
    if not f1:
        logger.error(f'{start_date} 格式无效,skip...')
        return
    elif not f2:
        logger.error(f'{end_date} 格式无效,skip...')
        return
    else:
        file_name = fetch_inquiry_data(start_date, end_date)
        if send_mail:
            subject = f'{start_date}~{end_date}问诊订单统计'
            content = f'内容详见附件'
            send_attachment_mail(subject=subject, content=content, files=file_name)
            clean_file(file_name)


if __name__ == '__main__':
    run()
