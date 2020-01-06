package org.github.liyibo1110.fakeguard;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public abstract class GuardException extends Exception{

	private List<OpResult> results;
	
	public interface CodeDeprecated {
	 	
		@Deprecated
        public static final int Ok = 0;

        @Deprecated
        public static final int SystemError = -1;
        
        @Deprecated
        public static final int RuntimeInconsistency = -2;
        
        @Deprecated
        public static final int DataInconsistency = -3;
        
        @Deprecated
        public static final int ConnectionLoss = -4;
        
        @Deprecated
        public static final int MarshallingError = -5;
       
        @Deprecated
        public static final int Unimplemented = -6;
        
        @Deprecated
        public static final int OperationTimeout = -7;
        
        @Deprecated
        public static final int BadArguments = -8;

        @Deprecated
        public static final int APIError = -100;

        @Deprecated
        public static final int NoNode = -101;
        
        @Deprecated
        public static final int NoAuth = -102;
        
        @Deprecated
        public static final int BadVersion = -103;
        
        @Deprecated
        public static final int NoChildrenForEphemerals = -108;
       
        @Deprecated
        public static final int NodeExists = -110;
       
        @Deprecated
        public static final int NotEmpty = -111;
       
        @Deprecated
        public static final int SessionExpired = -112;
      
        @Deprecated
        public static final int InvalidCallback = -113;
      
        @Deprecated
        public static final int InvalidACL = -114;
       
        @Deprecated
        public static final int AuthFailed = -115;
	}
	
	public static enum Code implements CodeDeprecated {
		
		OK (Ok),

        SYSTEMERROR (SystemError),

        RUNTIMEINCONSISTENCY (RuntimeInconsistency),
        
        DATAINCONSISTENCY (DataInconsistency),
        
        CONNECTIONLOSS (ConnectionLoss),
        
        MARSHALLINGERROR (MarshallingError),
        
        UNIMPLEMENTED (Unimplemented),
        
        OPERATIONTIMEOUT (OperationTimeout),
        
        BADARGUMENTS (BadArguments),
        
        APIERROR (APIError),

        NONODE (NoNode),
        
        NOAUTH (NoAuth),
        
        BADVERSION (BadVersion),
        
        NOCHILDRENFOREPHEMERALS (NoChildrenForEphemerals),
        
        NODEEXISTS (NodeExists),
        
        NOTEMPTY (NotEmpty),
        
        SESSIONEXPIRED (SessionExpired),
        
        INVALIDCALLBACK (InvalidCallback),
        
        INVALIDACL (InvalidACL),
        
        AUTHFAILED (AuthFailed),
        
        SESSIONMOVED (-118),
        
        NOTREADONLY (-119);
		
		private final int code;
		
		Code(int code) {
			this.code = code;
		}
		
		public int intValue() {
			return code;
		}
		
		private static final Map<Integer, Code> lookup = new HashMap<>();
		
		static {
			for (Code c : EnumSet.allOf(Code.class)) {
				lookup.put(c.code, c);
			}
		}
		
		public static Code get(int code) {
			return lookup.get(code);
		}
	}
	
	static String getCodeMessage(Code code) {
		 
		switch (code) {
	    	case OK:
	            return "ok";
	        case SYSTEMERROR:
	            return "SystemError";
	        case RUNTIMEINCONSISTENCY:
	            return "RuntimeInconsistency";
	        case DATAINCONSISTENCY:
	            return "DataInconsistency";
	        case CONNECTIONLOSS:
	            return "ConnectionLoss";
	        case MARSHALLINGERROR:
	            return "MarshallingError";
	        case UNIMPLEMENTED:
	            return "Unimplemented";
	        case OPERATIONTIMEOUT:
	            return "OperationTimeout";
	        case BADARGUMENTS:
	            return "BadArguments";
	        case APIERROR:
	            return "APIError";
	        case NONODE:
	            return "NoNode";
	        case NOAUTH:
	            return "NoAuth";
	        case BADVERSION:
	            return "BadVersion";
	        case NOCHILDRENFOREPHEMERALS:
	            return "NoChildrenForEphemerals";
	        case NODEEXISTS:
	            return "NodeExists";
	        case INVALIDACL:
	            return "InvalidACL";
	        case AUTHFAILED:
	            return "AuthFailed";
	        case NOTEMPTY:
	            return "Directory not empty";
	        case SESSIONEXPIRED:
	            return "Session expired";
	        case INVALIDCALLBACK:
	            return "Invalid callback";
	        case SESSIONMOVED:
	            return "Session moved";
	        case NOTREADONLY:
	            return "Not a read-only call";
	        default:
	            return "Unknown error " + code;
		}
	}
	
	private Code code;
	
	private String path;
	
	public GuardException(Code code) {
		this.code = code;
	}
	
	GuardException(Code code, String path) {
		this.code = code;
		this.path = path;
	}
	
	public Code code() {
		return code;
	}
	
	public String getPath() {
		return path;
	}
	
	@Override
	public String getMessage() {
		if (path == null) {
			return "GuardErrorCode = " + getCodeMessage(code);
		}
		return "GuardErrorCode = " + getCodeMessage(code) + " for " + path;
	}
	
	void setMultiResults(List<OpResult> results) {
		this.results = results;
	}
	
	/**
	 * 有的命令可能返回多条异常
	 * @return
	 */
	public List<OpResult> getResults() {
		return results != null ? new ArrayList<OpResult>(results) : null;
	}
	
	public static class APIErrorException extends GuardException {
        public APIErrorException() {
            super(Code.APIERROR);
        }
	}

    public static class AuthFailedException extends GuardException {
        public AuthFailedException() {
            super(Code.AUTHFAILED);
        }
    }

    public static class BadArgumentsException extends GuardException {
        public BadArgumentsException() {
            super(Code.BADARGUMENTS);
        }
        public BadArgumentsException(String path) {
            super(Code.BADARGUMENTS, path);
        }
    }

    public static class BadVersionException extends GuardException {
        public BadVersionException() {
            super(Code.BADVERSION);
        }
        public BadVersionException(String path) {
            super(Code.BADVERSION, path);
        }
    }

    public static class ConnectionLossException extends GuardException {
        public ConnectionLossException() {
            super(Code.CONNECTIONLOSS);
        }
    }

    public static class DataInconsistencyException extends GuardException {
        public DataInconsistencyException() {
            super(Code.DATAINCONSISTENCY);
        }
    }

    public static class InvalidACLException extends GuardException {
        public InvalidACLException() {
            super(Code.INVALIDACL);
        }
        public InvalidACLException(String path) {
            super(Code.INVALIDACL, path);
        }
    }

    public static class InvalidCallbackException extends GuardException {
        public InvalidCallbackException() {
            super(Code.INVALIDCALLBACK);
        }
    }

    public static class MarshallingErrorException extends GuardException {
        public MarshallingErrorException() {
            super(Code.MARSHALLINGERROR);
        }
    }

    public static class NoAuthException extends GuardException {
        public NoAuthException() {
            super(Code.NOAUTH);
        }
    }

    public static class NoChildrenForEphemeralsException extends GuardException {
        public NoChildrenForEphemeralsException() {
            super(Code.NOCHILDRENFOREPHEMERALS);
        }
        public NoChildrenForEphemeralsException(String path) {
            super(Code.NOCHILDRENFOREPHEMERALS, path);
        }
    }

    public static class NodeExistsException extends GuardException {
        public NodeExistsException() {
            super(Code.NODEEXISTS);
        }
        public NodeExistsException(String path) {
            super(Code.NODEEXISTS, path);
        }
    }

    public static class NoNodeException extends GuardException {
        public NoNodeException() {
            super(Code.NONODE);
        }
        public NoNodeException(String path) {
            super(Code.NONODE, path);
        }
    }

    public static class NotEmptyException extends GuardException {
        public NotEmptyException() {
            super(Code.NOTEMPTY);
        }
        public NotEmptyException(String path) {
            super(Code.NOTEMPTY, path);
        }
    }

    public static class OperationTimeoutException extends GuardException {
        public OperationTimeoutException() {
            super(Code.OPERATIONTIMEOUT);
        }
    }

    public static class RuntimeInconsistencyException extends GuardException {
        public RuntimeInconsistencyException() {
            super(Code.RUNTIMEINCONSISTENCY);
        }
    }

    public static class SessionExpiredException extends GuardException {
        public SessionExpiredException() {
            super(Code.SESSIONEXPIRED);
        }
    }

    public static class SessionMovedException extends GuardException {
        public SessionMovedException() {
            super(Code.SESSIONMOVED);
        }
    }

    public static class NotReadOnlyException extends GuardException {
        public NotReadOnlyException() {
            super(Code.NOTREADONLY);
        }
    }

    public static class SystemErrorException extends GuardException {
        public SystemErrorException() {
            super(Code.SYSTEMERROR);
        }
    }

    public static class UnimplementedException extends GuardException {
        public UnimplementedException() {
            super(Code.UNIMPLEMENTED);
        }
    }
}
