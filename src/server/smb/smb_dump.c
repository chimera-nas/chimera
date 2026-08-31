// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <inttypes.h>

#include "smb_dump.h"
#include "smb_internal.h"
#include "common/format.h"

static const char *
smb_command_name(uint32_t command)
{
    switch (command) {
        case SMB2_NEGOTIATE:
            return "Negotiate";
        case SMB2_SESSION_SETUP:
            return "SessionSetup";
        case SMB2_LOGOFF:
            return "Logoff";
        case SMB2_TREE_CONNECT:
            return "TreeConnect";
        case SMB2_TREE_DISCONNECT:
            return "TreeDisconnect";
        case SMB2_CREATE:
            return "Create";
        case SMB2_CLOSE:
            return "Close";
        case SMB2_FLUSH:
            return "Flush";
        case SMB2_READ:
            return "Read";
        case SMB2_WRITE:
            return "Write";
        case SMB2_LOCK:
            return "Lock";
        case SMB2_IOCTL:
            return "Ioctl";
        case SMB2_CANCEL:
            return "Cancel";
        case SMB2_ECHO:
            return "Echo";
        case SMB2_QUERY_DIRECTORY:
            return "QueryDirectory";
        case SMB2_CHANGE_NOTIFY:
            return "ChangeNotify";
        case SMB2_QUERY_INFO:
            return "QueryInfo";
        case SMB2_SET_INFO:
            return "SetInfo";
        case SMB2_OPLOCK_BREAK:
            return "OplockBreak";
        case SMB1_NEGOTIATE:
            return "SMB1Negotiate";
        default:
            return "Unknown";
    } /* switch */
} /* smb_command_name */

/* Every NTSTATUS the server can name, as data rather than a switch: the
 * corpus only ever produces a few dozen of these, and a thousand case
 * lines the tests can never reach say nothing useful about coverage.  The
 * scan is linear, but this is the debug-level dump path -- it never runs
 * at the default log level. */
/* *INDENT-OFF* */
static const struct {
    uint32_t    status;
    const char *name;
} smb_status_names[] = {
    { SMB2_STATUS_SUCCESS,                            "Success"     },
    { SMB2_STATUS_SHUTDOWN,                           "Shutdown"     },
    { SMB2_STATUS_PENDING,                            "Pending"     },
    { SMB2_STATUS_SMB_BAD_FID,                        "SmbBadFid"     },
    { SMB2_STATUS_NO_MORE_FILES,                      "NoMoreFiles"     },
    { SMB2_STATUS_UNSUCCESSFUL,                       "Unsuccessful"     },
    { SMB2_STATUS_NOT_IMPLEMENTED,                    "NotImplemented"     },
    { SMB2_STATUS_INVALID_INFO_CLASS,                 "InvalidInfoClass"     },
    { SMB2_STATUS_INFO_LENGTH_MISMATCH,               "InfoLengthMismatch"     },
    { SMB2_STATUS_ACCESS_VIOLATION,                   "AccessViolation"     },
    { SMB2_STATUS_IN_PAGE_ERROR,                      "InPageError"     },
    { SMB2_STATUS_PAGEFILE_QUOTA,                     "PagefileQuota"     },
    { SMB2_STATUS_INVALID_HANDLE,                     "InvalidHandle"     },
    { SMB2_STATUS_BAD_INITIAL_STACK,                  "BadInitialStack"     },
    { SMB2_STATUS_BAD_INITIAL_PC,                     "BadInitialPc"     },
    { SMB2_STATUS_INVALID_CID,                        "InvalidCid"     },
    { SMB2_STATUS_TIMER_NOT_CANCELED,                 "TimerNotCanceled"     },
    { SMB2_STATUS_INVALID_PARAMETER,                  "InvalidParameter"     },
    { SMB2_STATUS_NO_SUCH_DEVICE,                     "NoSuchDevice"     },
    { SMB2_STATUS_NO_SUCH_FILE,                       "NoSuchFile"     },
    { SMB2_STATUS_INVALID_DEVICE_REQUEST,             "InvalidDeviceRequest"     },
    { SMB2_STATUS_END_OF_FILE,                        "EndOfFile"     },
    { SMB2_STATUS_WRONG_VOLUME,                       "WrongVolume"     },
    { SMB2_STATUS_NO_MEDIA_IN_DEVICE,                 "NoMediaInDevice"     },
    { SMB2_STATUS_UNRECOGNIZED_MEDIA,                 "UnrecognizedMedia"     },
    { SMB2_STATUS_NONEXISTENT_SECTOR,                 "NonexistentSector"     },
    { SMB2_STATUS_MORE_PROCESSING_REQUIRED,           "MoreProcessingRequired"     },
    { SMB2_STATUS_NO_MEMORY,                          "NoMemory"     },
    { SMB2_STATUS_CONFLICTING_ADDRESSES,              "ConflictingAddresses"     },
    { SMB2_STATUS_NOT_MAPPED_VIEW,                    "NotMappedView"     },
    { SMB2_STATUS_UNABLE_TO_FREE_VM,                  "UnableToFreeVm"     },
    { SMB2_STATUS_UNABLE_TO_DELETE_SECTION,           "UnableToDeleteSection"     },
    { SMB2_STATUS_INVALID_SYSTEM_SERVICE,             "InvalidSystemService"     },
    { SMB2_STATUS_ILLEGAL_INSTRUCTION,                "IllegalInstruction"     },
    { SMB2_STATUS_INVALID_LOCK_SEQUENCE,              "InvalidLockSequence"     },
    { SMB2_STATUS_INVALID_VIEW_SIZE,                  "InvalidViewSize"     },
    { SMB2_STATUS_INVALID_FILE_FOR_SECTION,           "InvalidFileForSection"     },
    { SMB2_STATUS_ALREADY_COMMITTED,                  "AlreadyCommitted"     },
    { SMB2_STATUS_ACCESS_DENIED,                      "AccessDenied"     },
    { SMB2_STATUS_BUFFER_TOO_SMALL,                   "BufferTooSmall"     },
    { SMB2_STATUS_OBJECT_TYPE_MISMATCH,               "ObjectTypeMismatch"     },
    { SMB2_STATUS_NONCONTINUABLE_EXCEPTION,           "NoncontinuableException"     },
    { SMB2_STATUS_INVALID_DISPOSITION,                "InvalidDisposition"     },
    { SMB2_STATUS_UNWIND,                             "Unwind"     },
    { SMB2_STATUS_BAD_STACK,                          "BadStack"     },
    { SMB2_STATUS_INVALID_UNWIND_TARGET,              "InvalidUnwindTarget"     },
    { SMB2_STATUS_NOT_LOCKED,                         "NotLocked"     },
    { SMB2_STATUS_PARITY_ERROR,                       "ParityError"     },
    { SMB2_STATUS_UNABLE_TO_DECOMMIT_VM,              "UnableToDecommitVm"     },
    { SMB2_STATUS_NOT_COMMITTED,                      "NotCommitted"     },
    { SMB2_STATUS_INVALID_PORT_ATTRIBUTES,            "InvalidPortAttributes"     },
    { SMB2_STATUS_PORT_MESSAGE_TOO_LONG,              "PortMessageTooLong"     },
    { SMB2_STATUS_INVALID_PARAMETER_MIX,              "InvalidParameterMix"     },
    { SMB2_STATUS_INVALID_QUOTA_LOWER,                "InvalidQuotaLower"     },
    { SMB2_STATUS_DISK_CORRUPT_ERROR,                 "DiskCorruptError"     },
    { SMB2_STATUS_OBJECT_NAME_INVALID,                "ObjectNameInvalid"     },
    { SMB2_STATUS_OBJECT_NAME_NOT_FOUND,              "ObjectNameNotFound"     },
    { SMB2_STATUS_OBJECT_NAME_COLLISION,              "ObjectNameCollision"     },
    { SMB2_STATUS_HANDLE_NOT_WAITABLE,                "HandleNotWaitable"     },
    { SMB2_STATUS_PORT_DISCONNECTED,                  "PortDisconnected"     },
    { SMB2_STATUS_DEVICE_ALREADY_ATTACHED,            "DeviceAlreadyAttached"     },
    { SMB2_STATUS_OBJECT_PATH_INVALID,                "ObjectPathInvalid"     },
    { SMB2_STATUS_OBJECT_PATH_NOT_FOUND,              "ObjectPathNotFound"     },
    { SMB2_STATUS_OBJECT_PATH_SYNTAX_BAD,             "ObjectPathSyntaxBad"     },
    { SMB2_STATUS_DATA_OVERRUN,                       "DataOverrun"     },
    { SMB2_STATUS_DATA_LATE_ERROR,                    "DataLateError"     },
    { SMB2_STATUS_DATA_ERROR,                         "DataError"     },
    { SMB2_STATUS_CRC_ERROR,                          "CrcError"     },
    { SMB2_STATUS_SECTION_TOO_BIG,                    "SectionTooBig"     },
    { SMB2_STATUS_PORT_CONNECTION_REFUSED,            "PortConnectionRefused"     },
    { SMB2_STATUS_INVALID_PORT_HANDLE,                "InvalidPortHandle"     },
    { SMB2_STATUS_SHARING_VIOLATION,                  "SharingViolation"     },
    { SMB2_STATUS_QUOTA_EXCEEDED,                     "QuotaExceeded"     },
    { SMB2_STATUS_INVALID_PAGE_PROTECTION,            "InvalidPageProtection"     },
    { SMB2_STATUS_MUTANT_NOT_OWNED,                   "MutantNotOwned"     },
    { SMB2_STATUS_SEMAPHORE_LIMIT_EXCEEDED,           "SemaphoreLimitExceeded"     },
    { SMB2_STATUS_PORT_ALREADY_SET,                   "PortAlreadySet"     },
    { SMB2_STATUS_SECTION_NOT_IMAGE,                  "SectionNotImage"     },
    { SMB2_STATUS_SUSPEND_COUNT_EXCEEDED,             "SuspendCountExceeded"     },
    { SMB2_STATUS_THREAD_IS_TERMINATING,              "ThreadIsTerminating"     },
    { SMB2_STATUS_BAD_WORKING_SET_LIMIT,              "BadWorkingSetLimit"     },
    { SMB2_STATUS_INCOMPATIBLE_FILE_MAP,              "IncompatibleFileMap"     },
    { SMB2_STATUS_SECTION_PROTECTION,                 "SectionProtection"     },
    { SMB2_STATUS_EAS_NOT_SUPPORTED,                  "EasNotSupported"     },
    { SMB2_STATUS_EA_TOO_LARGE,                       "EaTooLarge"     },
    { SMB2_STATUS_NONEXISTENT_EA_ENTRY,               "NonexistentEaEntry"     },
    { SMB2_STATUS_NO_EAS_ON_FILE,                     "NoEasOnFile"     },
    { SMB2_STATUS_EA_CORRUPT_ERROR,                   "EaCorruptError"     },
    { SMB2_STATUS_FILE_LOCK_CONFLICT,                 "FileLockConflict"     },
    { SMB2_STATUS_LOCK_NOT_GRANTED,                   "LockNotGranted"     },
    { SMB2_STATUS_DELETE_PENDING,                     "DeletePending"     },
    { SMB2_STATUS_CTL_FILE_NOT_SUPPORTED,             "CtlFileNotSupported"     },
    { SMB2_STATUS_UNKNOWN_REVISION,                   "UnknownRevision"     },
    { SMB2_STATUS_REVISION_MISMATCH,                  "RevisionMismatch"     },
    { SMB2_STATUS_INVALID_OWNER,                      "InvalidOwner"     },
    { SMB2_STATUS_INVALID_PRIMARY_GROUP,              "InvalidPrimaryGroup"     },
    { SMB2_STATUS_NO_IMPERSONATION_TOKEN,             "NoImpersonationToken"     },
    { SMB2_STATUS_CANT_DISABLE_MANDATORY,             "CantDisableMandatory"     },
    { SMB2_STATUS_NO_LOGON_SERVERS,                   "NoLogonServers"     },
    { SMB2_STATUS_NO_SUCH_LOGON_SESSION,              "NoSuchLogonSession"     },
    { SMB2_STATUS_NO_SUCH_PRIVILEGE,                  "NoSuchPrivilege"     },
    { SMB2_STATUS_PRIVILEGE_NOT_HELD,                 "PrivilegeNotHeld"     },
    { SMB2_STATUS_INVALID_ACCOUNT_NAME,               "InvalidAccountName"     },
    { SMB2_STATUS_USER_EXISTS,                        "UserExists"     },
    { SMB2_STATUS_NO_SUCH_USER,                       "NoSuchUser"     },
    { SMB2_STATUS_GROUP_EXISTS,                       "GroupExists"     },
    { SMB2_STATUS_NO_SUCH_GROUP,                      "NoSuchGroup"     },
    { SMB2_STATUS_MEMBER_IN_GROUP,                    "MemberInGroup"     },
    { SMB2_STATUS_MEMBER_NOT_IN_GROUP,                "MemberNotInGroup"     },
    { SMB2_STATUS_LAST_ADMIN,                         "LastAdmin"     },
    { SMB2_STATUS_WRONG_PASSWORD,                     "WrongPassword"     },
    { SMB2_STATUS_ILL_FORMED_PASSWORD,                "IllFormedPassword"     },
    { SMB2_STATUS_PASSWORD_RESTRICTION,               "PasswordRestriction"     },
    { SMB2_STATUS_LOGON_FAILURE,                      "LogonFailure"     },
    { SMB2_STATUS_ACCOUNT_RESTRICTION,                "AccountRestriction"     },
    { SMB2_STATUS_INVALID_LOGON_HOURS,                "InvalidLogonHours"     },
    { SMB2_STATUS_INVALID_WORKSTATION,                "InvalidWorkstation"     },
    { SMB2_STATUS_PASSWORD_EXPIRED,                   "PasswordExpired"     },
    { SMB2_STATUS_ACCOUNT_DISABLED,                   "AccountDisabled"     },
    { SMB2_STATUS_NONE_MAPPED,                        "NoneMapped"     },
    { SMB2_STATUS_TOO_MANY_LUIDS_REQUESTED,           "TooManyLuidsRequested"     },
    { SMB2_STATUS_LUIDS_EXHAUSTED,                    "LuidsExhausted"     },
    { SMB2_STATUS_INVALID_SUB_AUTHORITY,              "InvalidSubAuthority"     },
    { SMB2_STATUS_INVALID_ACL,                        "InvalidAcl"     },
    { SMB2_STATUS_INVALID_SID,                        "InvalidSid"     },
    { SMB2_STATUS_INVALID_SECURITY_DESCR,             "InvalidSecurityDescr"     },
    { SMB2_STATUS_PROCEDURE_NOT_FOUND,                "ProcedureNotFound"     },
    { SMB2_STATUS_INVALID_IMAGE_FORMAT,               "InvalidImageFormat"     },
    { SMB2_STATUS_NO_TOKEN,                           "NoToken"     },
    { SMB2_STATUS_BAD_INHERITANCE_ACL,                "BadInheritanceAcl"     },
    { SMB2_STATUS_RANGE_NOT_LOCKED,                   "RangeNotLocked"     },
    { SMB2_STATUS_DISK_FULL,                          "DiskFull"     },
    { SMB2_STATUS_SERVER_DISABLED,                    "ServerDisabled"     },
    { SMB2_STATUS_SERVER_NOT_DISABLED,                "ServerNotDisabled"     },
    { SMB2_STATUS_TOO_MANY_GUIDS_REQUESTED,           "TooManyGuidsRequested"     },
    { SMB2_STATUS_INVALID_ID_AUTHORITY,               "InvalidIdAuthority"     },
    { SMB2_STATUS_AGENTS_EXHAUSTED,                   "AgentsExhausted"     },
    { SMB2_STATUS_INVALID_VOLUME_LABEL,               "InvalidVolumeLabel"     },
    { SMB2_STATUS_SECTION_NOT_EXTENDED,               "SectionNotExtended"     },
    { SMB2_STATUS_NOT_MAPPED_DATA,                    "NotMappedData"     },
    { SMB2_STATUS_RESOURCE_DATA_NOT_FOUND,            "ResourceDataNotFound"     },
    { SMB2_STATUS_RESOURCE_TYPE_NOT_FOUND,            "ResourceTypeNotFound"     },
    { SMB2_STATUS_RESOURCE_NAME_NOT_FOUND,            "ResourceNameNotFound"     },
    { SMB2_STATUS_ARRAY_BOUNDS_EXCEEDED,              "ArrayBoundsExceeded"     },
    { SMB2_STATUS_FLOAT_DENORMAL_OPERAND,             "FloatDenormalOperand"     },
    { SMB2_STATUS_FLOAT_DIVIDE_BY_ZERO,               "FloatDivideByZero"     },
    { SMB2_STATUS_FLOAT_INEXACT_RESULT,               "FloatInexactResult"     },
    { SMB2_STATUS_FLOAT_INVALID_OPERATION,            "FloatInvalidOperation"     },
    { SMB2_STATUS_FLOAT_OVERFLOW,                     "FloatOverflow"     },
    { SMB2_STATUS_FLOAT_STACK_CHECK,                  "FloatStackCheck"     },
    { SMB2_STATUS_FLOAT_UNDERFLOW,                    "FloatUnderflow"     },
    { SMB2_STATUS_INTEGER_DIVIDE_BY_ZERO,             "IntegerDivideByZero"     },
    { SMB2_STATUS_INTEGER_OVERFLOW,                   "IntegerOverflow"     },
    { SMB2_STATUS_PRIVILEGED_INSTRUCTION,             "PrivilegedInstruction"     },
    { SMB2_STATUS_TOO_MANY_PAGING_FILES,              "TooManyPagingFiles"     },
    { SMB2_STATUS_FILE_INVALID,                       "FileInvalid"     },
    { SMB2_STATUS_ALLOTTED_SPACE_EXCEEDED,            "AllottedSpaceExceeded"     },
    { SMB2_STATUS_INSUFFICIENT_RESOURCES,             "InsufficientResources"     },
    { SMB2_STATUS_DFS_EXIT_PATH_FOUND,                "DfsExitPathFound"     },
    { SMB2_STATUS_DEVICE_DATA_ERROR,                  "DeviceDataError"     },
    { SMB2_STATUS_DEVICE_NOT_CONNECTED,               "DeviceNotConnected"     },
    { SMB2_STATUS_DEVICE_POWER_FAILURE,               "DevicePowerFailure"     },
    { SMB2_STATUS_FREE_VM_NOT_AT_BASE,                "FreeVmNotAtBase"     },
    { SMB2_STATUS_MEMORY_NOT_ALLOCATED,               "MemoryNotAllocated"     },
    { SMB2_STATUS_WORKING_SET_QUOTA,                  "WorkingSetQuota"     },
    { SMB2_STATUS_MEDIA_WRITE_PROTECTED,              "MediaWriteProtected"     },
    { SMB2_STATUS_DEVICE_NOT_READY,                   "DeviceNotReady"     },
    { SMB2_STATUS_INVALID_GROUP_ATTRIBUTES,           "InvalidGroupAttributes"     },
    { SMB2_STATUS_BAD_IMPERSONATION_LEVEL,            "BadImpersonationLevel"     },
    { SMB2_STATUS_CANT_OPEN_ANONYMOUS,                "CantOpenAnonymous"     },
    { SMB2_STATUS_BAD_VALIDATION_CLASS,               "BadValidationClass"     },
    { SMB2_STATUS_BAD_TOKEN_TYPE,                     "BadTokenType"     },
    { SMB2_STATUS_BAD_MASTER_BOOT_RECORD,             "BadMasterBootRecord"     },
    { SMB2_STATUS_INSTRUCTION_MISALIGNMENT,           "InstructionMisalignment"     },
    { SMB2_STATUS_INSTANCE_NOT_AVAILABLE,             "InstanceNotAvailable"     },
    { SMB2_STATUS_PIPE_NOT_AVAILABLE,                 "PipeNotAvailable"     },
    { SMB2_STATUS_INVALID_PIPE_STATE,                 "InvalidPipeState"     },
    { SMB2_STATUS_PIPE_BUSY,                          "PipeBusy"     },
    { SMB2_STATUS_ILLEGAL_FUNCTION,                   "IllegalFunction"     },
    { SMB2_STATUS_PIPE_DISCONNECTED,                  "PipeDisconnected"     },
    { SMB2_STATUS_PIPE_CLOSING,                       "PipeClosing"     },
    { SMB2_STATUS_PIPE_CONNECTED,                     "PipeConnected"     },
    { SMB2_STATUS_PIPE_LISTENING,                     "PipeListening"     },
    { SMB2_STATUS_INVALID_READ_MODE,                  "InvalidReadMode"     },
    { SMB2_STATUS_IO_TIMEOUT,                         "IoTimeout"     },
    { SMB2_STATUS_FILE_FORCED_CLOSED,                 "FileForcedClosed"     },
    { SMB2_STATUS_PROFILING_NOT_STARTED,              "ProfilingNotStarted"     },
    { SMB2_STATUS_PROFILING_NOT_STOPPED,              "ProfilingNotStopped"     },
    { SMB2_STATUS_COULD_NOT_INTERPRET,                "CouldNotInterpret"     },
    { SMB2_STATUS_FILE_IS_A_DIRECTORY,                "FileIsADirectory"     },
    { SMB2_STATUS_NOT_SUPPORTED,                      "NotSupported"     },
    { SMB2_STATUS_REMOTE_NOT_LISTENING,               "RemoteNotListening"     },
    { SMB2_STATUS_DUPLICATE_NAME,                     "DuplicateName"     },
    { SMB2_STATUS_BAD_NETWORK_PATH,                   "BadNetworkPath"     },
    { SMB2_STATUS_NETWORK_BUSY,                       "NetworkBusy"     },
    { SMB2_STATUS_DEVICE_DOES_NOT_EXIST,              "DeviceDoesNotExist"     },
    { SMB2_STATUS_TOO_MANY_COMMANDS,                  "TooManyCommands"     },
    { SMB2_STATUS_ADAPTER_HARDWARE_ERROR,             "AdapterHardwareError"     },
    { SMB2_STATUS_INVALID_NETWORK_RESPONSE,           "InvalidNetworkResponse"     },
    { SMB2_STATUS_UNEXPECTED_NETWORK_ERROR,           "UnexpectedNetworkError"     },
    { SMB2_STATUS_BAD_REMOTE_ADAPTER,                 "BadRemoteAdapter"     },
    { SMB2_STATUS_PRINT_QUEUE_FULL,                   "PrintQueueFull"     },
    { SMB2_STATUS_NO_SPOOL_SPACE,                     "NoSpoolSpace"     },
    { SMB2_STATUS_PRINT_CANCELLED,                    "PrintCancelled"     },
    { SMB2_STATUS_NETWORK_NAME_DELETED,               "NetworkNameDeleted"     },
    { SMB2_STATUS_NETWORK_ACCESS_DENIED,              "NetworkAccessDenied"     },
    { SMB2_STATUS_BAD_DEVICE_TYPE,                    "BadDeviceType"     },
    { SMB2_STATUS_BAD_NETWORK_NAME,                   "BadNetworkName"     },
    { SMB2_STATUS_TOO_MANY_NAMES,                     "TooManyNames"     },
    { SMB2_STATUS_TOO_MANY_SESSIONS,                  "TooManySessions"     },
    { SMB2_STATUS_SHARING_PAUSED,                     "SharingPaused"     },
    { SMB2_STATUS_REQUEST_NOT_ACCEPTED,               "RequestNotAccepted"     },
    { SMB2_STATUS_REDIRECTOR_PAUSED,                  "RedirectorPaused"     },
    { SMB2_STATUS_NET_WRITE_FAULT,                    "NetWriteFault"     },
    { SMB2_STATUS_PROFILING_AT_LIMIT,                 "ProfilingAtLimit"     },
    { SMB2_STATUS_NOT_SAME_DEVICE,                    "NotSameDevice"     },
    { SMB2_STATUS_FILE_RENAMED,                       "FileRenamed"     },
    { SMB2_STATUS_VIRTUAL_CIRCUIT_CLOSED,             "VirtualCircuitClosed"     },
    { SMB2_STATUS_NO_SECURITY_ON_OBJECT,              "NoSecurityOnObject"     },
    { SMB2_STATUS_CANT_WAIT,                          "CantWait"     },
    { SMB2_STATUS_PIPE_EMPTY,                         "PipeEmpty"     },
    { SMB2_STATUS_CANT_ACCESS_DOMAIN_INFO,            "CantAccessDomainInfo"     },
    { SMB2_STATUS_CANT_TERMINATE_SELF,                "CantTerminateSelf"     },
    { SMB2_STATUS_INVALID_SERVER_STATE,               "InvalidServerState"     },
    { SMB2_STATUS_INVALID_DOMAIN_STATE,               "InvalidDomainState"     },
    { SMB2_STATUS_INVALID_DOMAIN_ROLE,                "InvalidDomainRole"     },
    { SMB2_STATUS_NO_SUCH_DOMAIN,                     "NoSuchDomain"     },
    { SMB2_STATUS_DOMAIN_EXISTS,                      "DomainExists"     },
    { SMB2_STATUS_DOMAIN_LIMIT_EXCEEDED,              "DomainLimitExceeded"     },
    { SMB2_STATUS_OPLOCK_NOT_GRANTED,                 "OplockNotGranted"     },
    { SMB2_STATUS_INVALID_OPLOCK_PROTOCOL,            "InvalidOplockProtocol"     },
    { SMB2_STATUS_INTERNAL_DB_CORRUPTION,             "InternalDbCorruption"     },
    { SMB2_STATUS_INTERNAL_ERROR,                     "InternalError"     },
    { SMB2_STATUS_GENERIC_NOT_MAPPED,                 "GenericNotMapped"     },
    { SMB2_STATUS_BAD_DESCRIPTOR_FORMAT,              "BadDescriptorFormat"     },
    { SMB2_STATUS_INVALID_USER_BUFFER,                "InvalidUserBuffer"     },
    { SMB2_STATUS_UNEXPECTED_IO_ERROR,                "UnexpectedIoError"     },
    { SMB2_STATUS_UNEXPECTED_MM_CREATE_ERR,           "UnexpectedMmCreateErr"     },
    { SMB2_STATUS_UNEXPECTED_MM_MAP_ERROR,            "UnexpectedMmMapError"     },
    { SMB2_STATUS_UNEXPECTED_MM_EXTEND_ERR,           "UnexpectedMmExtendErr"     },
    { SMB2_STATUS_NOT_LOGON_PROCESS,                  "NotLogonProcess"     },
    { SMB2_STATUS_LOGON_SESSION_EXISTS,               "LogonSessionExists"     },
    { SMB2_STATUS_INVALID_PARAMETER_1,                "InvalidParameter1"     },
    { SMB2_STATUS_INVALID_PARAMETER_2,                "InvalidParameter2"     },
    { SMB2_STATUS_INVALID_PARAMETER_3,                "InvalidParameter3"     },
    { SMB2_STATUS_INVALID_PARAMETER_4,                "InvalidParameter4"     },
    { SMB2_STATUS_INVALID_PARAMETER_5,                "InvalidParameter5"     },
    { SMB2_STATUS_INVALID_PARAMETER_6,                "InvalidParameter6"     },
    { SMB2_STATUS_INVALID_PARAMETER_7,                "InvalidParameter7"     },
    { SMB2_STATUS_INVALID_PARAMETER_8,                "InvalidParameter8"     },
    { SMB2_STATUS_INVALID_PARAMETER_9,                "InvalidParameter9"     },
    { SMB2_STATUS_INVALID_PARAMETER_10,               "InvalidParameter10"     },
    { SMB2_STATUS_INVALID_PARAMETER_11,               "InvalidParameter11"     },
    { SMB2_STATUS_INVALID_PARAMETER_12,               "InvalidParameter12"     },
    { SMB2_STATUS_REDIRECTOR_NOT_STARTED,             "RedirectorNotStarted"     },
    { SMB2_STATUS_REDIRECTOR_STARTED,                 "RedirectorStarted"     },
    { SMB2_STATUS_STACK_OVERFLOW,                     "StackOverflow"     },
    { SMB2_STATUS_NO_SUCH_PACKAGE,                    "NoSuchPackage"     },
    { SMB2_STATUS_BAD_FUNCTION_TABLE,                 "BadFunctionTable"     },
    { SMB2_STATUS_DIRECTORY_NOT_EMPTY,                "DirectoryNotEmpty"     },
    { SMB2_STATUS_FILE_CORRUPT_ERROR,                 "FileCorruptError"     },
    { SMB2_STATUS_NOT_A_DIRECTORY,                    "NotADirectory"     },
    { SMB2_STATUS_BAD_LOGON_SESSION_STATE,            "BadLogonSessionState"     },
    { SMB2_STATUS_LOGON_SESSION_COLLISION,            "LogonSessionCollision"     },
    { SMB2_STATUS_NAME_TOO_LONG,                      "NameTooLong"     },
    { SMB2_STATUS_FILES_OPEN,                         "FilesOpen"     },
    { SMB2_STATUS_CONNECTION_IN_USE,                  "ConnectionInUse"     },
    { SMB2_STATUS_MESSAGE_NOT_FOUND,                  "MessageNotFound"     },
    { SMB2_STATUS_PROCESS_IS_TERMINATING,             "ProcessIsTerminating"     },
    { SMB2_STATUS_INVALID_LOGON_TYPE,                 "InvalidLogonType"     },
    { SMB2_STATUS_NO_GUID_TRANSLATION,                "NoGuidTranslation"     },
    { SMB2_STATUS_CANNOT_IMPERSONATE,                 "CannotImpersonate"     },
    { SMB2_STATUS_IMAGE_ALREADY_LOADED,               "ImageAlreadyLoaded"     },
    { SMB2_STATUS_ABIOS_NOT_PRESENT,                  "AbiosNotPresent"     },
    { SMB2_STATUS_ABIOS_LID_NOT_EXIST,                "AbiosLidNotExist"     },
    { SMB2_STATUS_ABIOS_LID_ALREADY_OWNED,            "AbiosLidAlreadyOwned"     },
    { SMB2_STATUS_ABIOS_NOT_LID_OWNER,                "AbiosNotLidOwner"     },
    { SMB2_STATUS_ABIOS_INVALID_COMMAND,              "AbiosInvalidCommand"     },
    { SMB2_STATUS_ABIOS_INVALID_LID,                  "AbiosInvalidLid"     },
    { SMB2_STATUS_ABIOS_SELECTOR_NOT_AVAILABLE,       "AbiosSelectorNotAvailable"     },
    { SMB2_STATUS_ABIOS_INVALID_SELECTOR,             "AbiosInvalidSelector"     },
    { SMB2_STATUS_NO_LDT,                             "NoLdt"     },
    { SMB2_STATUS_INVALID_LDT_SIZE,                   "InvalidLdtSize"     },
    { SMB2_STATUS_INVALID_LDT_OFFSET,                 "InvalidLdtOffset"     },
    { SMB2_STATUS_INVALID_LDT_DESCRIPTOR,             "InvalidLdtDescriptor"     },
    { SMB2_STATUS_INVALID_IMAGE_NE_FORMAT,            "InvalidImageNeFormat"     },
    { SMB2_STATUS_RXACT_INVALID_STATE,                "RxactInvalidState"     },
    { SMB2_STATUS_RXACT_COMMIT_FAILURE,               "RxactCommitFailure"     },
    { SMB2_STATUS_MAPPED_FILE_SIZE_ZERO,              "MappedFileSizeZero"     },
    { SMB2_STATUS_TOO_MANY_OPENED_FILES,              "TooManyOpenedFiles"     },
    { SMB2_STATUS_CANCELLED,                          "Cancelled"     },
    { SMB2_STATUS_CANNOT_DELETE,                      "CannotDelete"     },
    { SMB2_STATUS_INVALID_COMPUTER_NAME,              "InvalidComputerName"     },
    { SMB2_STATUS_FILE_DELETED,                       "FileDeleted"     },
    { SMB2_STATUS_SPECIAL_ACCOUNT,                    "SpecialAccount"     },
    { SMB2_STATUS_SPECIAL_GROUP,                      "SpecialGroup"     },
    { SMB2_STATUS_SPECIAL_USER,                       "SpecialUser"     },
    { SMB2_STATUS_MEMBERS_PRIMARY_GROUP,              "MembersPrimaryGroup"     },
    { SMB2_STATUS_FILE_CLOSED,                        "FileClosed"     },
    { SMB2_STATUS_TOO_MANY_THREADS,                   "TooManyThreads"     },
    { SMB2_STATUS_THREAD_NOT_IN_PROCESS,              "ThreadNotInProcess"     },
    { SMB2_STATUS_TOKEN_ALREADY_IN_USE,               "TokenAlreadyInUse"     },
    { SMB2_STATUS_PAGEFILE_QUOTA_EXCEEDED,            "PagefileQuotaExceeded"     },
    { SMB2_STATUS_COMMITMENT_LIMIT,                   "CommitmentLimit"     },
    { SMB2_STATUS_INVALID_IMAGE_LE_FORMAT,            "InvalidImageLeFormat"     },
    { SMB2_STATUS_INVALID_IMAGE_NOT_MZ,               "InvalidImageNotMz"     },
    { SMB2_STATUS_INVALID_IMAGE_PROTECT,              "InvalidImageProtect"     },
    { SMB2_STATUS_INVALID_IMAGE_WIN_16,               "InvalidImageWin16"     },
    { SMB2_STATUS_LOGON_SERVER_CONFLICT,              "LogonServerConflict"     },
    { SMB2_STATUS_TIME_DIFFERENCE_AT_DC,              "TimeDifferenceAtDc"     },
    { SMB2_STATUS_SYNCHRONIZATION_REQUIRED,           "SynchronizationRequired"     },
    { SMB2_STATUS_DLL_NOT_FOUND,                      "DllNotFound"     },
    { SMB2_STATUS_OPEN_FAILED,                        "OpenFailed"     },
    { SMB2_STATUS_IO_PRIVILEGE_FAILED,                "IoPrivilegeFailed"     },
    { SMB2_STATUS_ORDINAL_NOT_FOUND,                  "OrdinalNotFound"     },
    { SMB2_STATUS_ENTRYPOINT_NOT_FOUND,               "EntrypointNotFound"     },
    { SMB2_STATUS_CONTROL_C_EXIT,                     "ControlCExit"     },
    { SMB2_STATUS_LOCAL_DISCONNECT,                   "LocalDisconnect"     },
    { SMB2_STATUS_REMOTE_DISCONNECT,                  "RemoteDisconnect"     },
    { SMB2_STATUS_REMOTE_RESOURCES,                   "RemoteResources"     },
    { SMB2_STATUS_LINK_FAILED,                        "LinkFailed"     },
    { SMB2_STATUS_LINK_TIMEOUT,                       "LinkTimeout"     },
    { SMB2_STATUS_INVALID_CONNECTION,                 "InvalidConnection"     },
    { SMB2_STATUS_INVALID_ADDRESS,                    "InvalidAddress"     },
    { SMB2_STATUS_DLL_INIT_FAILED,                    "DllInitFailed"     },
    { SMB2_STATUS_MISSING_SYSTEMFILE,                 "MissingSystemfile"     },
    { SMB2_STATUS_UNHANDLED_EXCEPTION,                "UnhandledException"     },
    { SMB2_STATUS_APP_INIT_FAILURE,                   "AppInitFailure"     },
    { SMB2_STATUS_PAGEFILE_CREATE_FAILED,             "PagefileCreateFailed"     },
    { SMB2_STATUS_NO_PAGEFILE,                        "NoPagefile"     },
    { SMB2_STATUS_INVALID_LEVEL,                      "InvalidLevel"     },
    { SMB2_STATUS_WRONG_PASSWORD_CORE,                "WrongPasswordCore"     },
    { SMB2_STATUS_ILLEGAL_FLOAT_CONTEXT,              "IllegalFloatContext"     },
    { SMB2_STATUS_PIPE_BROKEN,                        "PipeBroken"     },
    { SMB2_STATUS_REGISTRY_CORRUPT,                   "RegistryCorrupt"     },
    { SMB2_STATUS_REGISTRY_IO_FAILED,                 "RegistryIoFailed"     },
    { SMB2_STATUS_NO_EVENT_PAIR,                      "NoEventPair"     },
    { SMB2_STATUS_UNRECOGNIZED_VOLUME,                "UnrecognizedVolume"     },
    { SMB2_STATUS_SERIAL_NO_DEVICE_INITED,            "SerialNoDeviceInited"     },
    { SMB2_STATUS_NO_SUCH_ALIAS,                      "NoSuchAlias"     },
    { SMB2_STATUS_MEMBER_NOT_IN_ALIAS,                "MemberNotInAlias"     },
    { SMB2_STATUS_MEMBER_IN_ALIAS,                    "MemberInAlias"     },
    { SMB2_STATUS_ALIAS_EXISTS,                       "AliasExists"     },
    { SMB2_STATUS_LOGON_NOT_GRANTED,                  "LogonNotGranted"     },
    { SMB2_STATUS_TOO_MANY_SECRETS,                   "TooManySecrets"     },
    { SMB2_STATUS_SECRET_TOO_LONG,                    "SecretTooLong"     },
    { SMB2_STATUS_INTERNAL_DB_ERROR,                  "InternalDbError"     },
    { SMB2_STATUS_FULLSCREEN_MODE,                    "FullscreenMode"     },
    { SMB2_STATUS_TOO_MANY_CONTEXT_IDS,               "TooManyContextIds"     },
    { SMB2_STATUS_LOGON_TYPE_NOT_GRANTED,             "LogonTypeNotGranted"     },
    { SMB2_STATUS_NOT_REGISTRY_FILE,                  "NotRegistryFile"     },
    { SMB2_STATUS_NT_CROSS_ENCRYPTION_REQUIRED,       "NtCrossEncryptionRequired"     },
    { SMB2_STATUS_DOMAIN_CTRLR_CONFIG_ERROR,          "DomainCtrlrConfigError"     },
    { SMB2_STATUS_FT_MISSING_MEMBER,                  "FtMissingMember"     },
    { SMB2_STATUS_ILL_FORMED_SERVICE_ENTRY,           "IllFormedServiceEntry"     },
    { SMB2_STATUS_ILLEGAL_CHARACTER,                  "IllegalCharacter"     },
    { SMB2_STATUS_UNMAPPABLE_CHARACTER,               "UnmappableCharacter"     },
    { SMB2_STATUS_UNDEFINED_CHARACTER,                "UndefinedCharacter"     },
    { SMB2_STATUS_FLOPPY_VOLUME,                      "FloppyVolume"     },
    { SMB2_STATUS_FLOPPY_ID_MARK_NOT_FOUND,           "FloppyIdMarkNotFound"     },
    { SMB2_STATUS_FLOPPY_WRONG_CYLINDER,              "FloppyWrongCylinder"     },
    { SMB2_STATUS_FLOPPY_UNKNOWN_ERROR,               "FloppyUnknownError"     },
    { SMB2_STATUS_FLOPPY_BAD_REGISTERS,               "FloppyBadRegisters"     },
    { SMB2_STATUS_DISK_RECALIBRATE_FAILED,            "DiskRecalibrateFailed"     },
    { SMB2_STATUS_DISK_OPERATION_FAILED,              "DiskOperationFailed"     },
    { SMB2_STATUS_DISK_RESET_FAILED,                  "DiskResetFailed"     },
    { SMB2_STATUS_SHARED_IRQ_BUSY,                    "SharedIrqBusy"     },
    { SMB2_STATUS_FT_ORPHANING,                       "FtOrphaning"     },
    { SMB2_STATUS_PARTITION_FAILURE,                  "PartitionFailure"     },
    { SMB2_STATUS_INVALID_BLOCK_LENGTH,               "InvalidBlockLength"     },
    { SMB2_STATUS_DEVICE_NOT_PARTITIONED,             "DeviceNotPartitioned"     },
    { SMB2_STATUS_UNABLE_TO_LOCK_MEDIA,               "UnableToLockMedia"     },
    { SMB2_STATUS_UNABLE_TO_UNLOAD_MEDIA,             "UnableToUnloadMedia"     },
    { SMB2_STATUS_EOM_OVERFLOW,                       "EomOverflow"     },
    { SMB2_STATUS_NO_MEDIA,                           "NoMedia"     },
    { SMB2_STATUS_NO_SUCH_MEMBER,                     "NoSuchMember"     },
    { SMB2_STATUS_INVALID_MEMBER,                     "InvalidMember"     },
    { SMB2_STATUS_KEY_DELETED,                        "KeyDeleted"     },
    { SMB2_STATUS_NO_LOG_SPACE,                       "NoLogSpace"     },
    { SMB2_STATUS_TOO_MANY_SIDS,                      "TooManySids"     },
    { SMB2_STATUS_LM_CROSS_ENCRYPTION_REQUIRED,       "LmCrossEncryptionRequired"     },
    { SMB2_STATUS_KEY_HAS_CHILDREN,                   "KeyHasChildren"     },
    { SMB2_STATUS_CHILD_MUST_BE_VOLATILE,             "ChildMustBeVolatile"     },
    { SMB2_STATUS_DEVICE_CONFIGURATION_ERROR,         "DeviceConfigurationError"     },
    { SMB2_STATUS_DRIVER_INTERNAL_ERROR,              "DriverInternalError"     },
    { SMB2_STATUS_INVALID_DEVICE_STATE,               "InvalidDeviceState"     },
    { SMB2_STATUS_IO_DEVICE_ERROR,                    "IoDeviceError"     },
    { SMB2_STATUS_DEVICE_PROTOCOL_ERROR,              "DeviceProtocolError"     },
    { SMB2_STATUS_BACKUP_CONTROLLER,                  "BackupController"     },
    { SMB2_STATUS_LOG_FILE_FULL,                      "LogFileFull"     },
    { SMB2_STATUS_TOO_LATE,                           "TooLate"     },
    { SMB2_STATUS_NO_TRUST_LSA_SECRET,                "NoTrustLsaSecret"     },
    { SMB2_STATUS_NO_TRUST_SAM_ACCOUNT,               "NoTrustSamAccount"     },
    { SMB2_STATUS_TRUSTED_DOMAIN_FAILURE,             "TrustedDomainFailure"     },
    { SMB2_STATUS_TRUSTED_RELATIONSHIP_FAILURE,       "TrustedRelationshipFailure"     },
    { SMB2_STATUS_EVENTLOG_FILE_CORRUPT,              "EventlogFileCorrupt"     },
    { SMB2_STATUS_EVENTLOG_CANT_START,                "EventlogCantStart"     },
    { SMB2_STATUS_TRUST_FAILURE,                      "TrustFailure"     },
    { SMB2_STATUS_MUTANT_LIMIT_EXCEEDED,              "MutantLimitExceeded"     },
    { SMB2_STATUS_NETLOGON_NOT_STARTED,               "NetlogonNotStarted"     },
    { SMB2_STATUS_ACCOUNT_EXPIRED,                    "AccountExpired"     },
    { SMB2_STATUS_POSSIBLE_DEADLOCK,                  "PossibleDeadlock"     },
    { SMB2_STATUS_NETWORK_CREDENTIAL_CONFLICT,        "NetworkCredentialConflict"     },
    { SMB2_STATUS_REMOTE_SESSION_LIMIT,               "RemoteSessionLimit"     },
    { SMB2_STATUS_EVENTLOG_FILE_CHANGED,              "EventlogFileChanged"     },
    { SMB2_STATUS_NOLOGON_INTERDOMAIN_TRUST_ACCOUNT,  "NologonInterdomainTrustAccount"     },
    { SMB2_STATUS_NOLOGON_WORKSTATION_TRUST_ACCOUNT,  "NologonWorkstationTrustAccount"     },
    { SMB2_STATUS_NOLOGON_SERVER_TRUST_ACCOUNT,       "NologonServerTrustAccount"     },
    { SMB2_STATUS_DOMAIN_TRUST_INCONSISTENT,          "DomainTrustInconsistent"     },
    { SMB2_STATUS_FS_DRIVER_REQUIRED,                 "FsDriverRequired"     },
    { SMB2_STATUS_NO_USER_SESSION_KEY,                "NoUserSessionKey"     },
    { SMB2_STATUS_USER_SESSION_DELETED,               "UserSessionDeleted"     },
    { SMB2_STATUS_RESOURCE_LANG_NOT_FOUND,            "ResourceLangNotFound"     },
    { SMB2_STATUS_INSUFF_SERVER_RESOURCES,            "InsuffServerResources"     },
    { SMB2_STATUS_INVALID_BUFFER_SIZE,                "InvalidBufferSize"     },
    { SMB2_STATUS_INVALID_ADDRESS_COMPONENT,          "InvalidAddressComponent"     },
    { SMB2_STATUS_INVALID_ADDRESS_WILDCARD,           "InvalidAddressWildcard"     },
    { SMB2_STATUS_TOO_MANY_ADDRESSES,                 "TooManyAddresses"     },
    { SMB2_STATUS_ADDRESS_ALREADY_EXISTS,             "AddressAlreadyExists"     },
    { SMB2_STATUS_ADDRESS_CLOSED,                     "AddressClosed"     },
    { SMB2_STATUS_CONNECTION_DISCONNECTED,            "ConnectionDisconnected"     },
    { SMB2_STATUS_CONNECTION_RESET,                   "ConnectionReset"     },
    { SMB2_STATUS_TOO_MANY_NODES,                     "TooManyNodes"     },
    { SMB2_STATUS_TRANSACTION_ABORTED,                "TransactionAborted"     },
    { SMB2_STATUS_TRANSACTION_TIMED_OUT,              "TransactionTimedOut"     },
    { SMB2_STATUS_TRANSACTION_NO_RELEASE,             "TransactionNoRelease"     },
    { SMB2_STATUS_TRANSACTION_NO_MATCH,               "TransactionNoMatch"     },
    { SMB2_STATUS_TRANSACTION_RESPONDED,              "TransactionResponded"     },
    { SMB2_STATUS_TRANSACTION_INVALID_ID,             "TransactionInvalidId"     },
    { SMB2_STATUS_TRANSACTION_INVALID_TYPE,           "TransactionInvalidType"     },
    { SMB2_STATUS_NOT_SERVER_SESSION,                 "NotServerSession"     },
    { SMB2_STATUS_NOT_CLIENT_SESSION,                 "NotClientSession"     },
    { SMB2_STATUS_CANNOT_LOAD_REGISTRY_FILE,          "CannotLoadRegistryFile"     },
    { SMB2_STATUS_DEBUG_ATTACH_FAILED,                "DebugAttachFailed"     },
    { SMB2_STATUS_SYSTEM_PROCESS_TERMINATED,          "SystemProcessTerminated"     },
    { SMB2_STATUS_DATA_NOT_ACCEPTED,                  "DataNotAccepted"     },
    { SMB2_STATUS_NO_BROWSER_SERVERS_FOUND,           "NoBrowserServersFound"     },
    { SMB2_STATUS_VDM_HARD_ERROR,                     "VdmHardError"     },
    { SMB2_STATUS_DRIVER_CANCEL_TIMEOUT,              "DriverCancelTimeout"     },
    { SMB2_STATUS_REPLY_MESSAGE_MISMATCH,             "ReplyMessageMismatch"     },
    { SMB2_STATUS_MAPPED_ALIGNMENT,                   "MappedAlignment"     },
    { SMB2_STATUS_IMAGE_CHECKSUM_MISMATCH,            "ImageChecksumMismatch"     },
    { SMB2_STATUS_LOST_WRITEBEHIND_DATA,              "LostWritebehindData"     },
    { SMB2_STATUS_CLIENT_SERVER_PARAMETERS_INVALID,   "ClientServerParametersInvalid"     },
    { SMB2_STATUS_PASSWORD_MUST_CHANGE,               "PasswordMustChange"     },
    { SMB2_STATUS_NOT_FOUND,                          "NotFound"     },
    { SMB2_STATUS_NOT_TINY_STREAM,                    "NotTinyStream"     },
    { SMB2_STATUS_RECOVERY_FAILURE,                   "RecoveryFailure"     },
    { SMB2_STATUS_STACK_OVERFLOW_READ,                "StackOverflowRead"     },
    { SMB2_STATUS_FAIL_CHECK,                         "FailCheck"     },
    { SMB2_STATUS_DUPLICATE_OBJECTID,                 "DuplicateObjectid"     },
    { SMB2_STATUS_OBJECTID_EXISTS,                    "ObjectidExists"     },
    { SMB2_STATUS_CONVERT_TO_LARGE,                   "ConvertToLarge"     },
    { SMB2_STATUS_RETRY,                              "Retry"     },
    { SMB2_STATUS_FOUND_OUT_OF_SCOPE,                 "FoundOutOfScope"     },
    { SMB2_STATUS_ALLOCATE_BUCKET,                    "AllocateBucket"     },
    { SMB2_STATUS_PROPSET_NOT_FOUND,                  "PropsetNotFound"     },
    { SMB2_STATUS_MARSHALL_OVERFLOW,                  "MarshallOverflow"     },
    { SMB2_STATUS_INVALID_VARIANT,                    "InvalidVariant"     },
    { SMB2_STATUS_DOMAIN_CONTROLLER_NOT_FOUND,        "DomainControllerNotFound"     },
    { SMB2_STATUS_ACCOUNT_LOCKED_OUT,                 "AccountLockedOut"     },
    { SMB2_STATUS_HANDLE_NOT_CLOSABLE,                "HandleNotClosable"     },
    { SMB2_STATUS_CONNECTION_REFUSED,                 "ConnectionRefused"     },
    { SMB2_STATUS_GRACEFUL_DISCONNECT,                "GracefulDisconnect"     },
    { SMB2_STATUS_ADDRESS_ALREADY_ASSOCIATED,         "AddressAlreadyAssociated"     },
    { SMB2_STATUS_ADDRESS_NOT_ASSOCIATED,             "AddressNotAssociated"     },
    { SMB2_STATUS_CONNECTION_INVALID,                 "ConnectionInvalid"     },
    { SMB2_STATUS_CONNECTION_ACTIVE,                  "ConnectionActive"     },
    { SMB2_STATUS_NETWORK_UNREACHABLE,                "NetworkUnreachable"     },
    { SMB2_STATUS_HOST_UNREACHABLE,                   "HostUnreachable"     },
    { SMB2_STATUS_PROTOCOL_UNREACHABLE,               "ProtocolUnreachable"     },
    { SMB2_STATUS_PORT_UNREACHABLE,                   "PortUnreachable"     },
    { SMB2_STATUS_REQUEST_ABORTED,                    "RequestAborted"     },
    { SMB2_STATUS_CONNECTION_ABORTED,                 "ConnectionAborted"     },
    { SMB2_STATUS_BAD_COMPRESSION_BUFFER,             "BadCompressionBuffer"     },
    { SMB2_STATUS_USER_MAPPED_FILE,                   "UserMappedFile"     },
    { SMB2_STATUS_AUDIT_FAILED,                       "AuditFailed"     },
    { SMB2_STATUS_TIMER_RESOLUTION_NOT_SET,           "TimerResolutionNotSet"     },
    { SMB2_STATUS_CONNECTION_COUNT_LIMIT,             "ConnectionCountLimit"     },
    { SMB2_STATUS_LOGIN_TIME_RESTRICTION,             "LoginTimeRestriction"     },
    { SMB2_STATUS_LOGIN_WKSTA_RESTRICTION,            "LoginWkstaRestriction"     },
    { SMB2_STATUS_IMAGE_MP_UP_MISMATCH,               "ImageMpUpMismatch"     },
    { SMB2_STATUS_INSUFFICIENT_LOGON_INFO,            "InsufficientLogonInfo"     },
    { SMB2_STATUS_BAD_DLL_ENTRYPOINT,                 "BadDllEntrypoint"     },
    { SMB2_STATUS_BAD_SERVICE_ENTRYPOINT,             "BadServiceEntrypoint"     },
    { SMB2_STATUS_LPC_REPLY_LOST,                     "LpcReplyLost"     },
    { SMB2_STATUS_IP_ADDRESS_CONFLICT1,               "IpAddressConflict1"     },
    { SMB2_STATUS_IP_ADDRESS_CONFLICT2,               "IpAddressConflict2"     },
    { SMB2_STATUS_REGISTRY_QUOTA_LIMIT,               "RegistryQuotaLimit"     },
    { SMB2_STATUS_PATH_NOT_COVERED,                   "PathNotCovered"     },
    { SMB2_STATUS_NO_CALLBACK_ACTIVE,                 "NoCallbackActive"     },
    { SMB2_STATUS_LICENSE_QUOTA_EXCEEDED,             "LicenseQuotaExceeded"     },
    { SMB2_STATUS_PWD_TOO_SHORT,                      "PwdTooShort"     },
    { SMB2_STATUS_PWD_TOO_RECENT,                     "PwdTooRecent"     },
    { SMB2_STATUS_PWD_HISTORY_CONFLICT,               "PwdHistoryConflict"     },
    { SMB2_STATUS_PLUGPLAY_NO_DEVICE,                 "PlugplayNoDevice"     },
    { SMB2_STATUS_UNSUPPORTED_COMPRESSION,            "UnsupportedCompression"     },
    { SMB2_STATUS_INVALID_HW_PROFILE,                 "InvalidHwProfile"     },
    { SMB2_STATUS_INVALID_PLUGPLAY_DEVICE_PATH,       "InvalidPlugplayDevicePath"     },
    { SMB2_STATUS_DRIVER_ORDINAL_NOT_FOUND,           "DriverOrdinalNotFound"     },
    { SMB2_STATUS_DRIVER_ENTRYPOINT_NOT_FOUND,        "DriverEntrypointNotFound"     },
    { SMB2_STATUS_RESOURCE_NOT_OWNED,                 "ResourceNotOwned"     },
    { SMB2_STATUS_TOO_MANY_LINKS,                     "TooManyLinks"     },
    { SMB2_STATUS_QUOTA_LIST_INCONSISTENT,            "QuotaListInconsistent"     },
    { SMB2_STATUS_FILE_IS_OFFLINE,                    "FileIsOffline"     },
    { SMB2_STATUS_VOLUME_DISMOUNTED,                  "VolumeDismounted"     },
    { SMB2_STATUS_NOT_A_REPARSE_POINT,                "NotAReparsePoint"     },
    { SMB2_STATUS_SERVER_UNAVAILABLE,                 "ServerUnavailable"     },
    { SMB2_STATUS_BUFFER_OVERFLOW,                    "BufferOverflow"     },
    { SMB2_STATUS_STOPPED_ON_SYMLINK,                 "StoppedOnSymlink"     },
};
/* *INDENT-ON* */

static const char *
smb_status_name(uint32_t status)
{
    unsigned int i;

    for (i = 0; i < sizeof(smb_status_names) / sizeof(smb_status_names[0]);
         i++) {
        if (smb_status_names[i].status == status) {
            return smb_status_names[i].name;
        }
    }
    return "Unknown";
} /* smb_status_name */

static const char *
smb_create_disposition_name(uint32_t disposition)
{
    switch (disposition) {
        case SMB2_FILE_CREATE:
            return "Create";
        case SMB2_FILE_OPEN:
            return "Open";
        case SMB2_FILE_OPEN_IF:
            return "OpenIf";
        case SMB2_FILE_OVERWRITE:
            return "Overwrite";
        case SMB2_FILE_OVERWRITE_IF:
            return "OverwriteIf";
        default:
            return "Unknown";
    } /* switch */
} /* smb_create_disposition_name */

static const char *
smb_dialect_name(uint16_t dialect)
{
    switch (dialect) {
        case 0x0210:
            return "SMB2.1";
        case 0x02ff:
            return "SMB2.??";
        case 0x0300:
            return "SMB3.0";
        case 0x0302:
            return "SMB3.02";
        case 0x0311:
            return "SMB3.11";
        default:
            return "Unknown";
    } /* switch */

} /* smb_dialect_name */

static const char *
smb_ioctl_ctl_code_name(uint32_t ctl_code)
{
    switch (ctl_code) {
        case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
            return "VALIDATE_NEGOTIATE_INFO";
        case SMB2_FSCTL_TRANSCEIVE_PIPE:
            return "TRANSCEIVE_PIPE";
        case SMB2_FSCTL_DFS_GET_REFERRALS:
            return "DFS_GET_REFERRALS";
        default:
            return "Unknown";
    } /* switch */
} /* smb_ioctl_ctl_code_name */

void
_smb_dump_request(
    int                         i,
    int                         n,
    struct chimera_smb_request *request)
{
    char  argstr[512];
    char  hdr_args[160];
    char *hdrp = hdr_args;
    char  safe_path[256];
    char  safe_parent[200], safe_name[200];
    char  safe_pattern[256];

    *hdrp = '\0';

    if (request->smb2_hdr.session_id) {
        hdrp += sprintf(hdrp, " session %" PRIx64, request->smb2_hdr.session_id);
    }

    if (request->smb2_hdr.sync.tree_id) {
        if (request->tree) {
            sprintf(hdrp, " tree %s(%x)",
                    request->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE ? "IPC$" : request->tree->share->name,
                    request->smb2_hdr.sync.tree_id);
        } else {
            sprintf(hdrp, " tree_id %x", request->smb2_hdr.sync.tree_id);
        }
    }

    switch (request->smb2_hdr.command) {
        case SMB2_TREE_CONNECT:
            format_safe_name(safe_path, sizeof(safe_path), request->tree_connect.path, request->tree_connect.path_length
                             );
            snprintf(argstr, sizeof(argstr), " path %s", safe_path);
            break;
        case SMB2_CREATE:
            format_safe_name(safe_parent, sizeof(safe_parent), request->create.parent_path, request->create.
                             parent_path_len);
            format_safe_name(safe_name, sizeof(safe_name), request->create.name, request->create.name_len);
            snprintf(argstr, sizeof(argstr),
                     " parent_path %s name %s create_disposition %s create_options %x desired_access %x",
                     safe_parent,
                     safe_name,
                     smb_create_disposition_name(request->create.create_disposition),
                     request->create.create_options,
                     request->create.desired_access);
            break;
        case SMB2_CLOSE:
            if (request->close.file_id.pid != UINT64_MAX) {
                sprintf(argstr, " file_id %" PRIx64 ".%" PRIx64,
                        request->close.file_id.pid, request->close.file_id.vid);
            }
            break;
        case SMB2_WRITE:
            sprintf(argstr, " file_id %" PRIx64 ".%" PRIx64 " offset %" PRIu64 " length %u write_through %d",
                    request->write.file_id.pid, request->write.file_id.vid,
                    request->write.offset, request->write.length,
                    !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH));
            break;
        case SMB2_READ:
            sprintf(argstr, " file_id %" PRIx64 ".%" PRIx64 " offset %" PRIu64 " length %u",
                    request->read.file_id.pid, request->read.file_id.vid,
                    request->read.offset, request->read.length);
            break;
        case SMB2_IOCTL:
            sprintf(argstr, " file_id %" PRIx64 ".%" PRIx64 " ctl_code %s count %u",
                    request->ioctl.file_id.pid, request->ioctl.file_id.vid,
                    smb_ioctl_ctl_code_name(request->ioctl.ctl_code),
                    request->ioctl.input_count);
            break;
        case SMB2_SET_INFO:
            sprintf(argstr, " file_id %" PRIx64 ".%" PRIx64 " info_type %u info_class %u addl_info %u",
                    request->set_info.file_id.pid, request->set_info.file_id.vid,
                    request->set_info.info_type, request->set_info.info_class,
                    request->set_info.addl_info);
            break;
        case SMB2_QUERY_INFO:
            sprintf(argstr, " file_id %" PRIx64 ".%" PRIx64 " info_type %u info_class %u addl_info %u flags %u",
                    request->query_info.file_id.pid, request->query_info.file_id.vid,
                    request->query_info.info_type, request->query_info.info_class,
                    request->query_info.addl_info, request->query_info.flags);
            break;
        case SMB2_QUERY_DIRECTORY:
            format_safe_name(safe_pattern, sizeof(safe_pattern), request->query_directory.pattern, request->
                             query_directory.pattern_length);
            snprintf(argstr, sizeof(argstr), " file_id %" PRIx64 ".%" PRIx64
                     " flags %x info_class %u file_index %u pattern %s",
                     request->query_directory.file_id.pid, request->query_directory.file_id.vid,
                     request->query_directory.flags, request->query_directory.info_class,
                     request->query_directory.file_index,
                     safe_pattern);
            break;
        default:
            argstr[0] = '\0';
    } /* switch */

    chimera_smb_debug("SMB  Request %p: %d/%d Msgid %08x %s%s%s", request, i, n,
                      request->smb2_hdr.message_id,
                      smb_command_name(request->smb2_hdr.command),
                      hdr_args,
                      argstr);
} /* _smb_dump_request */ /* _smb_dump_request */

void
_smb_dump_reply(
    int                         i,
    int                         n,
    struct chimera_smb_request *request)
{
    char  argstr[512];
    char  hdr_args[80];
    char *hdrp = hdr_args;

    argstr[0] = '\0';

    switch (request->smb2_hdr.command) {
        case SMB2_NEGOTIATE:
            sprintf(argstr, " dialect %s", smb_dialect_name(request->negotiate.r_dialect));
            break;
        case SMB2_CREATE:
            if (request->status == SMB2_STATUS_SUCCESS) {
                sprintf(argstr, " file_id %" PRIx64 ".%" PRIx64,
                        request->create.r_open_file->file_id.pid,
                        request->create.r_open_file->file_id.vid);
            }
            break;
        default:
            break;
    } /* switch */

    *hdrp = '\0';

    if (request->session_handle && request->session_handle->session) {
        hdrp += sprintf(hdrp, " sessiond %" PRIx64, request->session_handle->session->session_id);
    }

    if (request->tree) {
        sprintf(hdrp, " tree_id %x", request->tree->tree_id);
    }

    chimera_smb_debug("SMB  Reply   %p: %d/%d MsgId %08x %s %s%s%s",
                      request, i, n, request->smb2_hdr.message_id,
                      smb_command_name(request->smb2_hdr.command),
                      smb_status_name(request->status), hdr_args, argstr);
} /* _smb_dump_request_reply */

void
_smb_dump_compound_request(struct chimera_smb_compound *compound)
{
    for (int i = 0; i < compound->num_requests; i++) {
        struct chimera_smb_request *request = compound->requests[i];
        _smb_dump_request(i + 1, compound->num_requests, request);
    }
} /* _smb_dump_compound_request */

void
_smb_dump_compound_reply(struct chimera_smb_compound *compound)
{
    for (int i = 0; i < compound->num_requests; i++) {
        struct chimera_smb_request *request = compound->requests[i];
        _smb_dump_reply(i + 1, compound->num_requests, request);
    }
} /* _smb_dump_compound_reply */