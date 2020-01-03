package org.github.liyibo1110.fakeguard.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PathTrie {

	private static final Logger LOG = LoggerFactory.getLogger(PathTrie.class);

	/**
	 * 树都得有根
	 */
	private final TrieNode rootNode;
	
	private static class TrieNode {
		
		/**
		 * 是否配置了配额
		 */
		boolean property = false;
		/**
		 * 所有子节点
		 */
		final HashMap<String, TrieNode> children;
		/**
		 * 父节点
		 */
		TrieNode parent = null;
		
		private TrieNode(TrieNode parent) {
			children = new HashMap<String, TrieNode>();
			this.parent = parent;
		}
		
		TrieNode getParent() {
			return this.parent;
		}
		
		void setParent(TrieNode parent) {
			this.parent = parent;
		}
		
		void setProperty(boolean prop) {
			this.property = prop;
		}
		
		boolean getProperty() {
			return this.property;
		}
		
		void addChild(String childName, TrieNode node) {
			synchronized (children) {
				if (children.containsKey(childName)) return;
				children.put(childName, node);
			}
		}
		
		void deleteChild(String childName) {
			synchronized (children) {
				if (!children.containsKey(childName)) return;
				TrieNode childNode = children.get(childName);
				if (childNode.getChildren().length == 1) {
					childNode.setParent(null);
					children.remove(childName);
				} else {
					// 如果有多个儿子，直接打标记，而不remove
					childNode.setProperty(false);
				}
			}
		}
		
		TrieNode getChild(String childName) {
			synchronized (children) {
				if (children.containsKey(childName)) {
					return children.get(childName);
				} else {
					return null;
				}
			}
		}
		
		/**
		 * 将复杂的Map类型转换成String[]
		 * @return
		 */
		String[] getChildren() {
			synchronized (children) {
				return children.keySet().toArray(new String[0]);
			}
		}
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Children of trienode: ");
			synchronized (children) {
				for (String str : children.keySet()) {
					sb.append(" " + str);
				}
			}
			return sb.toString();
		}
	}
	
	public PathTrie() {
		this.rootNode = new TrieNode(null);
	}
	
	public void addPath(String path) {
		if (path == null) return;
		String[] pathComponents = path.split("/");
		TrieNode parent = rootNode;
		String part = null;
		if (pathComponents.length <= 1) {	// 路径都是以/开头，所以不应该小于2
			throw new IllegalArgumentException("Invalid path " + path);
		}
		// 路径都是以/开头，所以目录要从下标1开始
		for (int i = 1; i < pathComponents.length; i++) {
			part = pathComponents[i];
			if (parent.getChild(part) == null) {
				parent.addChild(part, new TrieNode(parent));
			}
			// 换头
			parent = parent.getChild(part);
		}
		// 在路径最后一级标记配额开关
		parent.setProperty(true);
	}
	
	public void deletePath(String path) {
		if (path == null) return;
		String[] pathComponents = path.split("/");
		TrieNode parent = rootNode;
		String part = null;
		if (pathComponents.length <= 1) {	// 路径都是以/开头，所以不应该小于2
			throw new IllegalArgumentException("Invalid path " + path);
		}
		// 路径都是以/开头，所以目录要从下标1开始
		for (int i = 1; i < pathComponents.length; i++) {
			part = pathComponents[i];
			if (parent.getChild(part) == null) return;
			// 往下换头
			parent = parent.getChild(part);
			LOG.info("{}", parent);
		}
		// 删路径的最后一个
		TrieNode realParent = parent.getParent();
		realParent.deleteChild(part);
	}
	
	/**
	 * 核心查询方法，匹配最远的有配额的节点
	 * @param path
	 * @return
	 */
	public String findMaxPrefix(String path) {
		if (path == null) return null;
		if ("/".equals(path)) return path;
		String[] pathComponents = path.split("/");
		TrieNode parent = rootNode;
		List<String> components = new ArrayList<>();
		if (pathComponents.length <= 1) {	// 路径都是以/开头，所以不应该小于2
			throw new IllegalArgumentException("Invalid path " + path);
		}
		
		int i = 1;
		String part = null;
		StringBuilder sb = new StringBuilder();
		int lastIndex = -1;
		while (i < pathComponents.length) {
			if (parent.getChild(pathComponents[i]) != null) {
				part = pathComponents[i];
				parent = parent.getChild(part);
				components.add(part);
				if (parent.getProperty()) {
					lastIndex = i - 1;
				}
			} else {
				// 不匹配直接跳出
				break;
			}
			i++;
		}
		// 汇总匹配结果
		for (int j = 0; j < lastIndex + 1; j++) {
			sb.append("/" + components.get(j));
		}
		
		return sb.toString();
	}
	
	/**
	 * 清除所有节点
	 */
	public void clear() {
		// 因为deleteChild实现特殊，只删除第一层child即可
		for (String child : rootNode.getChildren()) {
			rootNode.deleteChild(child);
		}
	}
}
