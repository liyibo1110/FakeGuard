package org.github.liyibo1110.fakeguard;

import org.github.liyibo1110.fakeguard.data.Stat;

public abstract class OpResult {

	private int type;
	
	private OpResult(int type) {
		this.type = type;
	}
	
	public int getType() {
		return type;
	}
	
	public static class CreateResult extends OpResult {
		
		private String path;
		
		public CreateResult(String path) {
			super(FakeDefs.OpCode.create);
			this.path = path;
		}
		
		public String getPath() {
			return path;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof CreateResult)) return false;
			CreateResult other = (CreateResult)obj;
			return getType() == other.getType() && getPath().equals(other.getPath());
		}
		
		public int hashCode() {
			return getType() * 35 + path.hashCode(); 
		}
	}
	
	public static class DeleteResult extends OpResult {
		
		public DeleteResult() {
			super(FakeDefs.OpCode.delete);
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof DeleteResult)) return false;
			DeleteResult other = (DeleteResult)obj;
			return getType() == other.getType();
		}
		
		public int hashCode() {
			return getType(); 
		}
	}
	
	public static class SetDataResult extends OpResult {
		
		private Stat stat;
		
		public SetDataResult(Stat stat) {
			super(FakeDefs.OpCode.setData);
			this.stat = stat;
		}
		
		public Stat getStat() {
			return stat;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof SetDataResult)) return false;
			SetDataResult other = (SetDataResult)obj;
			return getType() == other.getType() && stat.getMzxid() == other.getStat().getMzxid();
		}
		
		public int hashCode() {
			return (int)(getType() * 35 + stat.getMzxid()); 
		}
	}
	
	public static class CheckResult extends OpResult {
		
		public CheckResult() {
			super(FakeDefs.OpCode.check);
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof CheckResult)) return false;
			CheckResult other = (CheckResult)obj;
			return getType() == other.getType();
		}
		
		public int hashCode() {
			return getType(); 
		}
	}
	
	public static class ErrorResult extends OpResult {
		
		private int error;
		
		public ErrorResult(int error) {
			super(FakeDefs.OpCode.error);
			this.error = error;
		}
		
		public int getError() {
			return error;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof ErrorResult)) return false;
			ErrorResult other = (ErrorResult)obj;
			return getType() == other.getType() && error == other.getError();
		}
		
		public int hashCode() {
			return getType() * 35 + error; 
		}
	}
}
