package org.github.liyibo1110.fakeguard;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.github.liyibo1110.fakeguard.data.ACL;
import org.github.liyibo1110.fakeguard.data.Id;

public class FakeDefs {

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
