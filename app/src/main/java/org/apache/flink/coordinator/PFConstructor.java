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
		if (pf.getYu()==0) pf.setYu(1);
		else pf.setYu(0);
	}

	public static void main(String[] args) {
		PFConstructor p=new PFConstructor();
		System.out.println(p.getPF().partition(3, 10));
		p.updatePF();
		System.out.println(p.getPF().partition(3, 10));
	}
}
