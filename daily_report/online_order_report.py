"""
线上订单汇总
统计的区间范围 上个自然月的数据
每月的一号自动发送
"""
import sys
import pandas as pd
import click
import re

sys.path.append('../')

from utils.db_utils import fetch_data
from utils.time_utils import *
from utils.mail_utils import send_attachment_mail
from utils.comm_utils import clean_file

logger = get_logger()


def __calculate_statistics_date(to_str=False):
    current_date = get_current_date_str()
    before_date = date_add(current_date, -1, to_str=to_str)
    start_date = str(before_date.year) + '-' + str(before_date.month).zfill(2) + str('-') + '01'
    return start_date, current_date


def __format_ord_state(val):
    state_mapping = {
        0: '待付款',
        1: '待发货',
        2: '部分发货（中药已发货）',
        3: '部分发货（西药/成药已发货）',
        4: '已发货（全部药品已发货）',
        5: '已取消',
        6: '已关闭'
    }
    return state_mapping[val] if val in state_mapping else '###'


def __format_pay_state(val):
    state_mapping = {
        0: '待支付',
        9: '预支付',
        1: '支付完成',
        5: '已退款'
    }
    return state_mapping[val] if val in state_mapping else '###'


def fetch_online_order_data(start_date, end_date):
    sql = f"""
        select
            inquiry_ord_id 订单编号,
            mobile_order.create_date  创建时间,
            medical_ord_id 结算单号,
            mobile_order.user_name 患者姓名,
            doctor.department 科室,
            user.bill_id 联系方式,
            mobile_order.doctor_name 开方医生,
            round(total_price / 10000,2) 支付金额,
            mobile_order.ord_state 订单状态,
            mobile_order.pay_state 付款状态,
            mobile_order.pay_date 付款时间
    from  ord.ord_mobile_medical mobile_order
    inner join  sec.sec_app_doctor doctor
    on mobile_order.doctor_id = doctor.id
    inner join user.usr_user user
    on mobile_order.user_id = user.user_id
    where
       -- binding_mechanism = '10000046'
     -- doctor.id  IN (SELECT id FROM sec.sec_app_doctor WHERE binding_mechanism = '10000046')
     doctor.id NOT IN(SELECT id FROM sec.sec_app_doctor WHERE binding_mechanism = '10001')
    -- and doctor.recommend_id not in (37,39)
    order by inquiry_ord_id desc
    """
    logger.info(f'query sql:{sql}')
    ord_df = fetch_data(sql)
    logger.info(f'total fetch {len(ord_df)}条记录')
    order_columns_origin = ord_df.columns.values.tolist()
    file_name = '线上订单.xlsx'
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    if len(ord_df) > 0:
        ord_df['订单状态'] = ord_df['订单状态'].map(__format_ord_state)
        ord_df['付款状态'] = ord_df['付款状态'].map(__format_pay_state)
        start_date_time = start_date + ' 00:00:00' if len(start_date) <= 10 else start_date
        end_date_time = end_date + ' 00:00:00' if len(end_date) <= 10 else end_date
        logger.info(f'start_date:{start_date_time}')
        logger.info(f'end_date:{end_date_time}')
        ord_df['创建时间'] = ord_df['创建时间'].astype('str')
        f1 = ord_df['创建时间'] > start_date_time
        f2 = ord_df['创建时间'] < end_date_time
        ord_df = ord_df[f1 & f2]
        logger.info(f'after filter:{len(ord_df)} 记录')
        ord_df.to_excel(writer, sheet_name='线上订单原始数据', index=False)
        ord_df['支付金额'] = ord_df['支付金额'].astype('float')
        tmp = ord_df.groupby(['开方医生', '患者姓名', '付款状态']).agg({'支付金额': 'sum'}).reset_index()
        tmp.rename(columns={'支付金额': '支付总金额'}, inplace=True)
        tmp.sort_values(by='支付总金额', ascending=False, inplace=True)
        tmp.to_excel(writer, sheet_name='订单汇总信息', index=False)
        # 设置单元格宽度
        writer.sheets['线上订单原始数据'].set_column(0, ord_df.shape[1] - 1, 20)
        writer.sheets['订单汇总信息'].set_column(0,tmp.shape[1] - 1, 20)
        writer.close()
    else:
        logger.warning(f'统计区间内未发现数据,将生成空的数据')
        sheet_one_df = pd.DataFrame(columns=order_columns_origin)
        sheet_two_df = pd.DataFrame(columns=['开方医生', '患者姓名', '付款状态', '支付总金额'])
        sheet_one_df.to_excel(writer, sheet_name='线上订单原始数据', index=False)
        sheet_two_df.to_excel(writer, sheet_name='订单汇总信息', index=False)
        writer.close()
    return file_name


@click.command()
@click.option('--start_date', default='2021-03-01', help='开始日期')
@click.option('--end_date', default='2021-04-01', help='结束日期')
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
        file_name = fetch_online_order_data(start_date, end_date)
        if send_mail:
            subject = f'{start_date}~{end_date}线上订单订单统计'
            content = f'内容详见附件'
            send_attachment_mail(subject=subject, content=content, files=file_name)
            clean_file(file_name)


if __name__ == '__main__':
    run()
