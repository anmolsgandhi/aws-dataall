import logging
from botocore.exceptions import ClientError
from .sts import SessionHelper


log = logging.getLogger(__name__)


class Athena:
    @staticmethod
    def client(account_id: str, region: str, role=None):
        session = SessionHelper.remote_session(accountid=account_id, role=role)
        return session.client('athena', region_name=region)

    @staticmethod
    def get_workgroup(AwsAccountId: str, region: str, workgroup: str, role=None):
        try:
            client = Athena.client(AwsAccountId, region, role)
            workgroup = client.get_work_group(WorkGroup=workgroup)
        except ClientError as e:
            log.info(
                f'Workgroup {workgroup} cannot be found'
                f'due to: {e}'
            )
        return workgroup
