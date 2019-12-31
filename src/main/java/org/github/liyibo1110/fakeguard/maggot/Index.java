package org.github.liyibo1110.fakeguard.maggot;

/**
 * 类似迭代器模式
 * @author liyibo
 *
 */
public interface Index {

	/**
	 * 是否读写完
	 * @return
	 */
	public boolean done();
	
	/**
	 * 换下一个数据
	 */
	public void incr();
}
