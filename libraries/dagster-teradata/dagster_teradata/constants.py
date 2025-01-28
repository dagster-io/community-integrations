"""Define constants for dagster-teradata."""

CC_OPR_SUCCESS_STATUS_MSG = "Compute Cluster %s  %s operation completed successfully."
CC_OPR_FAILURE_STATUS_MSG = "Compute Cluster %s  %s operation has failed."
CC_OPR_INITIALIZING_STATUS_MSG = (
    "The environment is currently initializing. Please wait."
)
CC_OPR_EMPTY_PROFILE_ERROR_MSG = (
    "Please provide a valid name for the compute cluster profile."
)
CC_GRP_PRP_NON_EXISTS_MSG = (
    "The specified Compute cluster is not present or The user doesn't have permission to "
    "access compute cluster."
)
CC_GRP_PRP_UN_AUTHORIZED_MSG = "The %s operation is not authorized for the user."
CC_GRP_LAKE_SUPPORT_ONLY_MSG = "Compute Groups is supported only on Vantage Cloud Lake."
CC_OPR_TIMEOUT_ERROR = "There is an issue with the %s operation. Kindly consult the administrator for assistance."
CC_GRP_PRP_EXISTS_MSG = "The specified Compute cluster is already exists."
CC_OPR_EMPTY_COPY_PROFILE_ERROR_MSG = (
    "Please provide a valid name for the source and target compute profile."
)
CC_OPR_TIME_OUT = 1200
CC_POLL_INTERVAL = 60
