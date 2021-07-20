package Reduce.Utils;

public class StudentPojo{
    private String name;
    private int age;
    private int classes;
    private double grade;


    public StudentPojo(int classes, double grade) {
        this.classes = classes;
        this.grade = grade;
    }

    public StudentPojo(String name, int age, int classes, double grade) {
        this.name = name;
        this.age = age;
        this.classes = classes;
        this.grade = grade;
    }

    public StudentPojo(){}

    public StudentPojo(String name, int age, int classes) {
        this.name = name;
        this.age = age;
        this.classes = classes;
    }

    public double getGrade() {
        return grade;
    }

    public void setGrade(double grade) {
        this.grade = grade;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getClasses() {
        return classes;
    }

    public void setClasses(int classes) {
        this.classes = classes;
    }

    public String toString(){

        return "My name is " + name + " class " + classes + " age " + age + " grade avg " + grade;
    }
}