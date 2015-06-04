package edu.pku.dblab.parallel;

import java.util.Set;

public class LabeledVertex {
	
	private int id;
	private Set<Integer> labels;
	
	public int getId(){
		return this.id;
	}
	
	public void setId(int id){
		this.id = id;
	}
	
	public Set<Integer> getLabels(){
		return this.labels;
	}
	
	public void setLabels(Set<Integer> labels){
		this.labels = labels;
	}
	
	public boolean containsLabel(Integer label){
		for(Integer content:labels){
			if(content.intValue() == label.intValue()){
				return true;
			}
		}
		return false;
	}

}
