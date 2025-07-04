from functools import cached_property
from pathlib import Path

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import BaseNotifier
from airflow.providers.smtp.hooks.smtp import SmtpHook


def dag_notifier(subject:str, html_content:str, from_email:str = None, to:str = None):

    with SmtpHook('smtp_default') as h:
        if from_email is None:
            from_email = h.from_email

    if to is None:
        to = "airflowalert@rhyta.com"

    return SmtpNotifier(from_email=from_email,
                        to=to,
                        subject=subject,
                        html_content=html_content)


dag_failed = dag_notifier(
    subject="[Error] The dag {{ dag.dag_id }} failed",
    html_content='Hi from Airflow !\n{{ dag.dag_id }} finished as: Failed. Please check log.')

dag_success = dag_notifier(
    subject = "[Success] The dag {{ dag.dag_id }} has been executed",
    html_content='Hi from Airflow !\n{{ dag.dag_id }} finished as: Success.')


class MyTaskNotifier(BaseNotifier):
    """Basic notifier, prints the task_id, state and a message."""

    def __init__(self, from_email:str = None, to:str = None, subject:str = None, html_content:str = None):

        self.from_email = from_email
        self.to = to
        self.subject = subject
        self.html_content= html_content

    @staticmethod
    def _read_template(template_path: str) -> str:
        return Path(template_path).read_text().replace("\n", "").strip()

    @cached_property
    def hook(self) -> SmtpHook:

        return SmtpHook(smtp_conn_id='smtp_default')

    def notify(self, context) -> None:

        ti = context['ti']
        t_id = ti.task_id
        t_state = ti.state
        t_tries = f'{ti.try_number}/{ti.max_tries+1}'
        self.to = 'airflowalert@rhyta.com'
        self.subject = f'[Error] The task {t_id} failed'
        self.html_content = f'Hi from MyNotifier !\n{t_id} finished as: {t_state} with {t_tries} tries. Please check log.'
        #self.html_content= self._read_template('./utils/task_template.html')

        print(
            f"MYNOTIFIER !!!!"
        )

        with self.hook as smtp:
            smtp.send_email_smtp(
                from_email=smtp.from_email,
                to=self.to,
                subject=self.subject,
                html_content=self.html_content
            )
