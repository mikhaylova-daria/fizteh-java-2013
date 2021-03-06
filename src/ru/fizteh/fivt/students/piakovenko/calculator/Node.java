package ru.fizteh.fivt.students.piakovenko.calculator;

import java.io.IOException;

public class Node {
    private String s;
    private Node rightNode;
    private Node leftNode;

    private int calculation(int dec) throws IOException {
        int leftNodeNumber = 0;
        int rightNodeNumber = 0;
        try {
            leftNodeNumber = Integer.parseInt(leftNode.getString(), dec);
            rightNodeNumber = Integer.parseInt(rightNode.getString(), dec);
        } catch (NumberFormatException e){
            System.err.println(e.getMessage());
            System.exit(4);
        }
        if (s.equals("*")) {
            if (leftNodeNumber != 0  && Integer.MAX_VALUE /leftNodeNumber < rightNodeNumber) {
                throw(new IOException("Ovefflow of integer"));
            }
            return leftNodeNumber * rightNodeNumber;
        } else if (s.equals("/")) {
            if (rightNodeNumber == 0 ) {
                throw(new IOException("Trying divide by zero"));
            }
            return leftNodeNumber / rightNodeNumber;
        } else if (s.equals("-")) {
            if ( Integer.signum(leftNodeNumber) == Integer.signum(rightNodeNumber) ) {
                if (Integer.MAX_VALUE - leftNodeNumber < rightNodeNumber) {
                    throw(new IOException("Ovefflow of integer"));
                }
            }
            return leftNodeNumber - rightNodeNumber;
        } else {
            if ( Integer.signum(leftNodeNumber) == Integer.signum(rightNodeNumber) ) {
                   if (Integer.MAX_VALUE - leftNodeNumber < rightNodeNumber) {
                        throw(new IOException("Ovefflow of integer"));
                   }
            }
            return leftNodeNumber + rightNodeNumber;
        }
    }

    public Node(String s1) {
        this.s = s1;
        this.leftNode = new Node();
        this.rightNode = new Node();
    }

    public Node() {
        this.s = "";
    }

    public boolean isEmpty() {
        return s.isEmpty();
    }

    public void addRightNode(Node n) {
        this.rightNode = n;
    }

    public void addLeftNode(Node n) {
        this.leftNode = n;
    }
    public String getString() {
        return s;
    }

    public boolean equal(String t) {
        return this.s.equals(t);
    }

    public void calculate() throws IOException {
        if (this.leftNode.isEmpty() && this.rightNode.isEmpty()){
            return;
        }
        if (!this.leftNode.isEmpty()) {
            leftNode.calculate();
        }
        if ( !this.rightNode.isEmpty()) {
            rightNode.calculate();
        }
        this.s = Integer.toString(calculation(19), 19);
        leftNode.s = "";
        rightNode.s = "";
    }
}
