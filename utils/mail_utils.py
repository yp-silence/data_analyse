"""
simple send mail utils
"""

import smtplib
import emoji

from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from utils.logger_utils import get_logger
from utils.config_utils import parse_config

logger = get_logger()

mail_config_dict = parse_config(key='mail_config')


def send_text_mail(subject: str, content: str, to: str):
    """
    send text mail
    :param subject: 邮件主题
    :param content: 邮件内容
    :param to: 邮件接收者 mail_add1,mail_add2...
    :return:
    """
    mail_from = mail_config_dict['user']
    mail_to = to.split(',')
    host = mail_config_dict.get('host')
    port = mail_config_dict.get('port')
    pwd = mail_config_dict.get('password')
    msg = MIMEText(content, _charset='utf-8')
    msg['From'] = Header(mail_from, 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    try:
        smtp_server = smtplib.SMTP_SSL(host, port)
        smtp_server.login(mail_from, pwd)
        logger.info('login mail server success')
        smtp_server.sendmail(mail_from, mail_to, msg.as_string())
        logger.info(f'mail deliver to {to} success')
    except smtplib.SMTPException:
        logger.error('mail send failure')
    else:
        smtp_server.quit()


def send_attachment_mail(subject: str, content: str, files: (list, str), to=None):
    """
    发送带附件的邮件
    :param to: 邮件的收件人
    :param subject: 邮件主题
    :param content: 邮件内容
    :param files: 附件对象 文件名列表
    :return:
    """
    mail_from = mail_config_dict['user']
    to = to if to else mail_config_dict['to']
    mail_to = to.split(',')
    host = mail_config_dict.get('host')
    port = mail_config_dict.get('port')
    pwd = mail_config_dict.get('password')
    message = MIMEMultipart()
    message['From'] = Header(mail_from, 'utf-8')
    message['Subject'] = Header(subject, 'utf-8')
    # add mail content
    message.attach(MIMEText(content, _subtype='plain', _charset='utf-8'))
    # add attach object
    if isinstance(files, str):
        files = [files]
    for path in files:
        att = MIMEApplication(open(path, 'rb').read())
        # attach file name
        file_name = path[path.rfind('/') + 1:] if '/' in path else path
        att.add_header('Content-Disposition', 'attachment', filename=file_name)
        message.attach(att)
    try:
        smtp_server = smtplib.SMTP_SSL(host, port)
        smtp_server.login(mail_from, pwd)
        logger.info('login mail server success')
        smtp_server.sendmail(mail_from, mail_to, message.as_string())
        logger.info(f'mail deliver to {to} success')
    except smtplib.SMTPException:
        logger.error('mail send failure')
    else:
        smtp_server.quit()
        logger.info(f'mail send to {to} success~')


def test_run():
    subject = emoji.emojize(':red_heart:')
    content = 'test send_text_mail' + emoji.emojize(':books:')
    to = '157241092@qq.com'
    files = ['/Users/rm-it/PycharmProjects/data_analyse/utils/app-bind.js', '若邻云诊所-数据库表定义.xlsx']
    send_attachment_mail(subject, content, to, files)


if __name__ == '__main__':
    test_run()

