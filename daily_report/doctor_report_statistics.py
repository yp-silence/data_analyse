"""
医生报道参数统计
"""
from utils.db_utils import fetch_data
from utils.logger_utils import get_logger
from utils.time_utils import get_current_date_str

import pandas as pd
import click

logger = get_logger()


def get_first_date_to_target_opt(start_date, end_date, target=20, is_md=False, to_excel=True, brand_id=10000347,
                                 merge=False):
    """10001070"""
    sql = f"""
            SELECT 
                doctor.name name,
                user_rel.doctor_id, 
                user_rel.create_date date_time
             FROM user.usr_doctor_user_rel user_rel inner join sec.sec_app_doctor doctor
             on user_rel.doctor_id = doctor.id
             WHERE user_rel.state > 0 and user_rel.brand_id = {brand_id}
             and
                user_rel.create_date between  '{start_date}' AND '{end_date}'
            order by user_rel.create_date
            """
    df = fetch_data(sql, is_md=is_md)
    count_df = df.groupby(['name', 'doctor_id']).agg({'date_time': 'count'}).reset_index()
    count_df.rename(columns={'date_time': "累计新增报道数"}, inplace=True)
    if target == 1:
        count_df['统计区间'] = "[" + start_date + '→' + end_date + ")"
        count_df.rename(columns={"name": "医生姓名"}, inplace=True)
        target_df = count_df
    else:
        # 统计首次达到阈值的时间
        tmp = df.groupby(['name', 'doctor_id']).shift(target - 1).reset_index()
        # 去除 nan索引的记录
        tmp = tmp.dropna()
        logger.info(f'tmp:{tmp}')
        df = df.loc[tmp['index'].values]
        target_df = df.groupby(['name', 'doctor_id']).agg({"date_time": 'min'}).reset_index()
        target_df = count_df.merge(target_df, on=['name', 'doctor_id'], how='inner')
        target_df.rename(columns={"name": "医生姓名", "date_time": "首次达到阈值时间"}, inplace=True)
        target_df['新增报道人数阈值'] = target
        target_df['统计区间'] = "[" + start_date + '→' + end_date + ")"
        logger.info(f'total find {len(target_df)}条记录')
    doctor_ids = target_df['doctor_id'].values.tolist()
    doctor_ids = ["'" + str(val) + "'" for val in doctor_ids]
    logger.info(f'doctor_ids:{doctor_ids}')
    detail_df = get_detail_info(doctor_ids, start_date, end_date, brand_id, is_md=is_md)
    if to_excel:
        if merge:
            io = pd.ExcelWriter(f'{get_current_date_str()}医生报道量统计.xlsx')
            target_df.drop(labels='doctor_id', inplace=True, axis=1)
            sheet_name = '新增报道数统计' if target == 1 else '首次达到阈值统计'
            target_df.to_excel(io, index=False, sheet_name=sheet_name)
            detail_df.to_excel(io, index=False, sheet_name='详细信息')
            io.sheets[sheet_name].set_column(0, target_df.shape[1], 20)
            io.sheets['详细信息'].set_column(0, detail_df.shape[1], 20)
            io.close()
        else:
            file_name = f"{start_date}-{end_date}医生新增报道量统计.xlsx" if target == 1 else f"{start_date}-{end_date}医生报道量阈值统计.xlsx"
            io = pd.ExcelWriter(file_name)
            target_df.drop(labels=['doctor_id'], axis=1, inplace=True)
            sheet_name = '新增报道数统计' if target == 1 else '首次达到阈值统计'
            target_df.to_excel(io, index=False, sheet_name=sheet_name)
            io.sheets[sheet_name].set_column(0, target_df.shape[1], 20)
            io.close()
            io = pd.ExcelWriter(f"{start_date}-{end_date}医生报道详情统计.xlsx")
            detail_df.to_excel(io, sheet_name='详详细信息', index=False)
            io.sheets['详详细信息'].set_column(0, target_df.shape[1], 20)
            io.close()


def get_detail_info(doctor_ids, start_date, end_date, brand_id=10000347, is_md=False):
    """brand_id=10000347 --> 若邻医生互联网医院"""
    if isinstance(doctor_ids, list):
        doctor_ids = ','.join(doctor_ids)
    sql = f"""
        select 
        doctor.name HCP姓名, usr.user_name NP姓名,if(usr.user_sex = 1, '男', '女') 性别, usr.bill_id 联系电话, 
        usr_rel.create_date 成功报道时间
        from user.usr_doctor_user_rel usr_rel
             inner join user.usr_user usr on usr_rel.user_id = usr.user_id
             left join sec.sec_app_doctor doctor on usr_rel.doctor_id = doctor.id
    where doctor_id in ({doctor_ids})
      and usr_rel.brand_id = {brand_id}
      and usr_rel.create_date between '{start_date}' AND '{end_date}'
    order by HCP姓名, 成功报道时间 desc
        """
    detail_df = fetch_data(sql, is_md=is_md)
    logger.info(f'total find {len(detail_df)}条记录')
    if len(detail_df) > 0:
        return detail_df
    else:
        return pd.DataFrame(columns=['HCP姓名', 'NP姓名', '性别', '联系电话', '成功报道时间'])


def get_first_date_to_target(start_date, end_date, target=20, is_md=False, to_excel=False, brand_id=10000347):
    """获取首先达到阈值的时间"""
    sql = f"""
             select 
                   doctor.name, usr_rel.create_date
            from user.usr_doctor_user_rel usr_rel
            inner join sec.sec_app_doctor doctor
            on usr_rel.doctor_id = doctor.id
            where usr_rel.create_date between '{start_date}' AND '{end_date}'
            and usr_rel.doctor_id in (
                select doctor_id
                    from (
                             SELECT doctor_id,
                                    count(user_id) as 新增报道量
                             FROM user.usr_doctor_user_rel
                             WHERE state > 0 and brand_id = {brand_id}
                             and create_date BETWEEN '{start_date}' AND '{end_date}'
                             group by doctor_id 
                         ) tmp
                        where tmp.新增报道量 >= {target})
            order by doctor.name, create_date;
        """
    tmp_df = fetch_data(sql, is_md=is_md)

    def filter_rank(df):
        return df[df.create_date.rank(method='dense', ascending=True) == target]

    tmp_df = tmp_df.groupby('name').apply(filter_rank).reset_index(drop=True)
    tmp_df.rename(columns={"create_date": "首次达到阈值时间", "name": "医生姓名"}, inplace=True)
    tmp_df['报道人数阈值'] = target
    tmp_df['统计时间区间'] = start_date + '→' + end_date

    tmp_df = tmp_df[['医生姓名', '报道人数阈值', '首次达到阈值时间', '统计时间区间']]
    logger.info(f'find {len(tmp_df)}条记录')
    if to_excel:
        io = pd.ExcelWriter(f'{get_current_date_str()}医生报道量阈值统计.xlsx')
        tmp_df.to_excel(io, index=False)
        io.sheets['Sheet1'].set_column(0, tmp_df.shape[1], 20)
        io.close()


@click.command()
@click.option('--start_date', help='开始日期', default='2021-04-21 08:00:00')
@click.option('--end_date', help='结束日期', default='2021-04-21 18:00:00')
@click.option('--is_md', help='是否明德', default=False)
@click.option('--target', help='阈值', default=1)
@click.option('--to_excel', help='是否生成excel', default=True)
@click.option('--merge', help='是否合并excel', default=False)
@click.option('--brand_id', help='机构id', default=10000347)
def run(start_date, end_date, is_md, target, to_excel, merge, brand_id):
    get_first_date_to_target_opt(start_date, end_date, is_md=is_md, target=target, to_excel=to_excel,
                                 merge=merge, brand_id=brand_id)


if __name__ == '__main__':
    run()