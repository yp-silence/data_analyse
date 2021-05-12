"""
医生报表数据 每周五下午两点自动发送
统计周期
"""
import sys
import click
import pandas as pd
import re

# 导入上级目录自定义的包
sys.path.append('../')
print(sys.path)

from utils.db_utils import fetch_data
from utils.time_utils import *
from utils.mail_utils import send_attachment_mail
from utils.comm_utils import clean_file

logg = get_logger()


def __get_statistics_date():
    current_date = get_current_date_str()
    end_date = date_add(current_date, 1,to_str=True)
    start_date = date_add(current_date, -6,to_str=True)
    logg.info(f'statistics date range {start_date}~{end_date}')
    return start_date, end_date


def fetch_report_data(start_date, end_date):
    """"""
    sql = f"""
             SELECT sec_app_doctor.name 医生姓名,sec_recommend.name 推荐人,medical_institution 所在医院 ,
             department 所在科室,关注总数,报道总数,'{start_date}' 开始日期,'{end_date}' 结束日期 , 新增关注量,新增报道量,新增咨询量,新增医嘱数,
             线上支付,线下支付
    FROM 
     sec.sec_app_doctor  
    LEFT JOIN(
        SELECT doctor_id,count(distinct account_id) 关注总数, count(IF(create_date BETWEEN '{start_date}' AND '{end_date}',1,null)) as 新增关注量 
        FROM  user.usr_doctor_account_rel WHERE state=1 group by doctor_id
        ) as follow ON sec_app_doctor.id=follow.doctor_id
    LEFT JOIN 
    (SELECT doctor_id,count(distinct user_id) as 报道总数, count(IF(create_date BETWEEN '{start_date}' AND '{end_date}',1,null)) as 新增报道量
        FROM user.usr_doctor_user_rel  WHERE state>0 group by doctor_id
        ) as doc_usr ON doc_usr.doctor_id=follow.doctor_id
    -- 新增咨询量
    LEFT JOIN
    (SELECT doctor_id,count(*) as 新增咨询量 
        FROM ord.ord_inquiry WHERE create_date BETWEEN '{start_date}' AND '{end_date}'  group by doctor_id
        )AS doc_ord ON doc_ord.doctor_id=follow.doctor_id
    LEFT JOIN
    -- 医嘱数
    (SELECT doctor_id,count(*) 新增医嘱数,count(IF(pay_state=1 AND pay_type=1,1,null)) as 线下支付,count(IF(pay_state=1 AND pay_type=2,1,null)) as 线上支付 
        FROM ord.ord_mobile_prescription WHERE create_date BETWEEN '{start_date}' AND '{end_date}'
        AND state=1  group by doctor_id
        )AS doc_rp ON doc_rp.doctor_id=follow.doctor_id
    
    LEFT JOIN 
        sec.sec_recommend ON sec_app_doctor.recommend_id=sec_recommend.recommend_id
    -- WHERE  sec_app_doctor.id  IN (SELECT id FROM sec.sec_app_doctor WHERE binding_mechanism = '10000046') 
     -- 去掉测试机构  邻家好医
     WHERE  sec_app_doctor.id NOT IN (SELECT id FROM sec.sec_app_doctor WHERE binding_mechanism = '10001')    
    ORDER BY 关注总数 desc;
    """

    df = fetch_data(sql)
    logg.info(f'total fetch {len(df)} 条记录')
    logg.info(f'sample data : {df.head()}')
    # 筛选数据
    df['推荐人'] = df['推荐人'].astype(str)
    name_filter = df['推荐人'].str.contains('李胜国|郑法宗')
    df = df[name_filter]
    df.sort_values(by=['新增关注量', '新增报道量', '新增咨询量', '新增医嘱数', '线上支付', '线下支付'], ascending=False, inplace=True)
    file_name = f'医生报表数据-{start_date}~{end_date}.xlsx'
    io = pd.ExcelWriter(file_name)
    df.to_excel(io, index=False)
    io.sheets['Sheet1'].set_column('A:N', 20)
    logg.info(f'generate file {file_name} success ~')
    io.close()
    return file_name


@click.command()
@click.option('--start', default='2021-03-01', help='开始日期')
@click.option('--end', default='2021-04-17', help='结束日期')
@click.option('--send_mail', default=False, help='是否发送邮件')
def run(start, end, send_mail):
    if start == '' and end == '':
        start, end = __get_statistics_date()
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    compile = re.compile(pattern)
    f1 = bool(compile.match(start))
    f2 = bool(compile.match(end))
    if not f1:
        logg.error(f'{start} 格式无效,skip...')
        return
    elif not f2:
        logg.error(f'{end} 格式无效,skip...')
        return
    else:
        file_name = fetch_report_data(start, end)
        if send_mail:
            subject = f'{start}-{end} 医生关注和新增数据统计报表'
            content = '内容详见附件'
            files = file_name
            send_attachment_mail(subject, content, files)
            clean_file(file_name)


if __name__ == '__main__':
    run()
