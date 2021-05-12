import pandas as pd


def format_profile(path='template.xlsx'):
    """"""
    df = pd.read_excel(path)
    columns_ordered = ['年', '月', '诊所名称', '诊疗费', '辅助检查',
                       '西／成药处方', '中药处方', '治疗费', '实验室检查',
                       '材料费', '其他收费', '收入合计',
                       '诊疗费成本', '辅助检查成本',
                       '西／成药处方成本', '中药处方成本', '治疗成本', '实验室检查成本',
                       '材料费成本', '其他收费成本', '成本合计',
                       '诊疗费利润', '辅助检查利润',
                       '西／成药处方利润', '中药处方利润', '治疗费利润', '实验室检查利润',
                       '材料费利润', '其他收费利润', '利润合计'
                       ]
    df = df[columns_ordered]
    print(df.head(1).T)
    pass


def run():
    format_profile()


if __name__ == '__main__':
    run()
