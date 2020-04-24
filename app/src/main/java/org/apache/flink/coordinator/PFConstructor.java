package org.apache.flink.coordinator;

class PFConstructor {
	private MyPF pf;
	PFConstructor() {
		pf=new MyPF<Integer>();
	}
	MyPF getPF() {
		return pf;
	}
	void updatePF() {

	}

}
