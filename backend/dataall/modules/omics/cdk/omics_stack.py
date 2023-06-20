""""
Creates a CloudFormation stack for Omics pipelines using CDK
"""
import logging
import os
import shutil

from aws_cdk import (
    aws_sagemaker as sagemaker,
    aws_ec2 as ec2,
    aws_kms as kms,
    aws_iam as iam,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    pipelines,
    Stack,
    CfnOutput,
)
from aws_cdk.aws_s3_assets import Asset

from dataall.modules.omics.db.models import OmicsPipeline
from dataall.modules.omics.db import models
from dataall.db.models import EnvironmentGroup

from dataall.cdkproxy.stacks.manager import stack
from dataall.db import Engine, get_engine
from dataall.db.api import Environment
from dataall.utils.cdk_nag_utils import CDKNagUtil
from dataall.utils.runtime_stacks_tagging import TagsUtil

logger = logging.getLogger(__name__)


@stack(stack='omics_pipeline')
class OmicsPipelineStack(Stack):
    """
    Creation of an Omics pipeline stack.
    Having imported the Omics module, the class registers itself using @stack
    Then it will be reachable by HTTP request / using SQS from GraphQL lambda
    """

    module_name = __file__

    def get_engine(self) -> Engine:
        envname = os.environ.get('envname', 'local')
        engine = get_engine(envname=envname)
        return engine

    def get_target(self, target_uri) -> OmicsPipeline:
        engine = self.get_engine()
        with engine.scoped_session() as session:
            pipeline = session.query(OmicsPipeline).get(target_uri)
        return pipeline

    def get_pipeline_environment(
        self, pipeline: OmicsPipeline
    ) -> models.Environment:
        envname = os.environ.get('envname', 'local')
        engine = self.get_engine(envname=envname)
        with engine.scoped_session() as session:
            return Environment.get_environment_by_uri(session, pipeline.environmentUri)


    def get_env_group(
        self, pipeline: OmicsPipeline
    ) -> EnvironmentGroup:
        engine = self.get_engine()
        with engine.scoped_session() as session:
            env = Environment.get_environment_group(
                session, pipeline.SamlAdminGroupName, pipeline.environmentUri
            )
        return env

    def __init__(self, scope, id: str, target_uri: str = None, **kwargs) -> None:
        super().__init__(scope,
                         id,
                         description="Cloud formation stack of OMICS PIPELINE: {}; URI: {}; DESCRIPTION: {}".format(
                             self.get_target(target_uri=target_uri).label,
                             target_uri,
                             self.get_target(target_uri=target_uri).description,
                         )[:1024],
                         **kwargs)

        # Required for dynamic stack tagging
        self.target_uri = target_uri

        pipeline = self.get_target(target_uri=target_uri)
        pipeline_environment = self.get_pipeline_environment(pipeline)
        env_group = self.get_env_group(pipeline)

        #TODO: Define stack if needed
        resource_prefix = f"{pipeline_environment.resourcePrefix}-omics-{pipeline.OmicsPipelineUri}"[:63]

        code_dir_path = os.path.realpath(
            os.path.abspath(
                os.path.join(
                    __file__, '..', '..', '..', '..', 'blueprints', 'omics_pipeline'
                )
            )
        )
        OmicsPipelineStack.write_cdk_app_file(code_dir_path, pipeline, pipeline_environment)

        OmicsPipelineStack.cleanup_zip_directory(code_dir_path)

        OmicsPipelineStack.zip_directory(code_dir_path)

        code_asset = Asset(
            scope=self, id=f'{pipeline.name}-asset', path=f'{code_dir_path}/code.zip'
        )
        code_repo = codecommit.Repository(
            self,
            "Repository",
            repository_name=resource_prefix,
            code=codecommit.Code.from_asset(code_asset),
            description=f"Code repository for pipeline {resource_prefix}. Generated by data.all."
        )
        self.pipeline_iam_role = iam.Role(
            self,
            id=f"{resource_prefix}-pipeline-role",
            role_name=f"{resource_prefix}-pipeline-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("codebuild.amazonaws.com"),
                iam.ServicePrincipal("codepipeline.amazonaws.com"),
                iam.AccountPrincipal(self.account),
            ),
        )
        self.codebuild_policy = [
            iam.PolicyStatement(
                actions=[
                    "sts:GetServiceBearerToken",
                ],
                resources=["*"],
                conditions={
                    "StringEquals": {"sts:AWSServiceName": "codeartifact.amazonaws.com"}
                },
            ),
            iam.PolicyStatement(
                actions=[
                    "ecr:GetAuthorizationToken",
                ],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=[
                    "codeartifact:GetAuthorizationToken",
                    "codeartifact:GetRepositoryEndpoint",
                    "codeartifact:ReadFromRepository",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:PutImage",
                    "ecr:InitiateLayerUpload",
                    "ecr:UploadLayerPart",
                    "ecr:CompleteLayerUpload",
                    "ecr:GetDownloadUrlForLayer",
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:GenerateDataKey",
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                    "ssm:GetParametersByPath",
                    "ssm:GetParameters",
                    "ssm:GetParameter",
                    "s3:Get*",
                    "s3:Put*",
                    "s3:List*",
                    "codebuild:CreateReportGroup",
                    "codebuild:CreateReport",
                    "codebuild:UpdateReport",
                    "codebuild:BatchPutTestCases",
                    "codebuild:BatchPutCodeCoverages",
                ],
                resources=[
                    f"arn:aws:s3:::{resource_prefix}*",
                    f"arn:aws:s3:::{resource_prefix}*/*",
                    f"arn:aws:codebuild:{self.region}:{self.account}:project/*{resource_prefix}*",
                    f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:*{resource_prefix}*",
                    f"arn:aws:kms:{self.region}:{self.account}:key/*",
                    f"arn:aws:ssm:*:{self.account}:parameter/*{resource_prefix}*",
                    f"arn:aws:ecr:{self.region}:{self.account}:repository/{resource_prefix}*",
                    f"arn:aws:codeartifact:{self.region}:{self.account}:repository/{resource_prefix}*",
                    f"arn:aws:codeartifact:{self.region}:{self.account}:domain/{resource_prefix}*",
                ],
            ),
        ]
        for policy in self.codebuild_policy:
            self.pipeline_iam_role.add_to_policy(policy)

        self.pipeline = pipelines.CodePipeline(
            self,
            f"{resource_prefix}-pipeline",
            pipeline_name=resource_prefix,
            publish_assets_in_parallel=False,
            self_mutation=True,
            synth=pipelines.CodeBuildStep(
                "Synth",
                input=pipelines.CodePipelineSource.code_commit(
                    repository=code_repo,
                    branch="main",
                ),
                build_environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.AMAZON_LINUX_2_3,
                    environment_variables={
                        'PIPELINE_URI': codebuild.BuildEnvironmentVariable(
                            value=pipeline.OmicsPipelineUri
                        ),
                        'ENV_RESOURCE_PREFIX': codebuild.BuildEnvironmentVariable(
                            value=pipeline_environment.resourcePrefix
                        )
                    }
                ),
                commands=[
                    "n 16",
                    "npm install -g aws-cdk",
                    "pip install -r requirements.txt",
                    "cdk synth",
                ],
                role_policy_statements=self.codebuild_policy,
            ),
            cross_account_keys=True,
        )

        #TODO

        TagsUtil.add_tags(stack=self, model=models.OmicsPipeline, target_type="omics_pipeline")

        CDKNagUtil.check_rules(self)


@staticmethod
def zip_directory(path):
    try:
        shutil.make_archive("code", "zip", path)
        shutil.move("code.zip", f"{path}/code.zip")
    except Exception as e:
        logger.error(f"Failed to zip repository due to: {e}")


@staticmethod
def cleanup_zip_directory(path):
    if os.path.isfile(f"{path}/code.zip"):
        os.remove(f"{path}/code.zip")
    else:
        logger.info("Info: %s Zip not found" % f"{path}/code.zip")


@staticmethod
def write_cdk_app_file(code_dir_path, pipeline, environment):
    app = f"""
    #!/usr/bin/env python3
    import os
    import uuid

    import aws_cdk as cdk
    import boto3
    from cdk_nag import AwsSolutionsChecks, NagSuppressions, NagPackSuppression

    from omics_pipeline.stacks.cdk_nag_exclusions import PIPELINE_STACK_CDK_NAG_EXCLUSIONS
    from omics_pipeline.omics_pipeline_stack import OmicsPipelineStack


    pipeline_data = dict(
        pipeline_name="{pipeline.name}",
        pipeline_uri="{pipeline.OmicsPipelineUri}",
        environment_name="{environment.name}",
        environment_uri="{environment.environmentUri}",
        environment_resourcePrefix="{environment.resourcePrefix}",
        input_bucket="{pipeline.S3InputBucket}",
        input_prefix="{pipeline.S3InputPrefix}",
        output_bucket="{pipeline.S3OutputBucket}",
        output_prefix="{pipeline.S3OutputPrefix}",
    )
    print("Omics Pipeline Data", pipeline_data)
    resource_prefix = "{environment.resourcePrefix}-omics-{pipeline.OmicsPipelineUri}"[:63]
    env_resource_prefix = "{environment.resourcePrefix}"   
    app = cdk.App()

    account_id = boto3.client("sts").get_caller_identity().get("Account") or os.getenv(
        "CDK_DEFAULT_ACCOUNT"
    )
    cdk_pipeline_region = app.node.try_get_context("tooling_region") or os.getenv(
        "CDK_DEFAULT_REGION"
    )
    git_branch = "main"
    env = cdk.Environment(account=account_id, region=cdk_pipeline_region)

    pipeline = OmicsPipelineStack(
        app,
        resource_prefix,
        env=env,
        git_branch=git_branch,
        resource_prefix=resource_prefix,
        commit_id=os.getenv("COMMIT_ID", str(uuid.uuid4())[:8]),
        repository_name=resource_prefix,
        env_resource_prefix=env_resource_prefix,
        input_bucket="{pipeline.S3InputBucket}",
        input_prefix="{pipeline.S3InputPrefix}",
        output_bucket="{pipeline.S3OutputBucket}",
        output_prefix="{pipeline.S3OutputPrefix}",
        tags=dict(
            OmicsPipelineUri="{pipeline.OmicsPipelineUri}",
            OmicsPipelineLabel="{pipeline.OmicsPipelineUri}",
            OmicsPipelineName="{pipeline.name}",
            Team="{pipeline.SamlGroupName}",
        )
    )
    cdk.Aspects.of(app).add(AwsSolutionsChecks(reports=True, verbose=False))
    NagSuppressions.add_stack_suppressions(
        pipeline,
        suppressions=[
            NagPackSuppression(id=rule_suppressed["id"], reason=rule_suppressed["reason"])
            for rule_suppressed in PIPELINE_STACK_CDK_NAG_EXCLUSIONS
        ],
        apply_to_nested_stacks=True,
    )
    app.synth()

    """
    with open(f'{code_dir_path}/app.py', 'w') as text_file:
        text_file.write(textwrap.dedent(app))
        print(app)

