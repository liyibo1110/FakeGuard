package org.github.liyibo1110.fakeguard;

import java.util.ArrayList;
import java.util.Collections;

import org.github.liyibo1110.fakeguard.data.ACL;
import org.github.liyibo1110.fakeguard.data.Id;

public class FakeDefs {

	public interface OpCode {
		
		public final int notification = 0;
		
		public final int create = 1;
		
		public final int delete = 2;
		
		public final int exists = 3;
		
		public final int getData = 4;

	    public final int setData = 5;

        public final int getACL = 6;

        public final int setACL = 7;

        public final int getChildren = 8;

        public final int sync = 9;

        public final int ping = 11;

        public final int getChildren2 = 12;

        public final int check = 13;

        public final int multi = 14;

        public final int auth = 100;

        public final int setWatches = 101;

        public final int sasl = 102;

        public final int createSession = -10;

        public final int closeSession = -11;

        public final int error = -1;
	}
	
	public interface Perms {
		int READ = 1 << 0;
		
		int WRITE = 1 << 1;
		
		int CREATE = 1 << 2;
		
		int DELETE = 1 << 3;
		
		int ADMIN = 1 << 4;
		
		int ALL = READ | WRITE | CREATE | DELETE | ADMIN;
	}
	
	public interface Ids {
		
		public final Id ANYONE_ID_UNSAFE = new Id("world", "anyone");
		
		public final Id AUTH_IDS = new Id("auth", "");
		
		public final ArrayList<ACL> OPEN_ACL_UNSAFE = new ArrayList<>(
				Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID_UNSAFE))
		);
		
		public final ArrayList<ACL> CREATOR_ALL_ACL = new ArrayList<>(
				Collections.singletonList(new ACL(Perms.ALL, AUTH_IDS))
		);
		
		public final ArrayList<ACL> READ_ACL_UNSAFE = new ArrayList<>(
				Collections.singletonList(new ACL(Perms.READ, ANYONE_ID_UNSAFE))
		);
	}
}
