package com.jujin.kafka.kafkademo.text;

import java.util.Objects;

/**
 * @author MSI
 * 测试深克隆
 */
public class textClone {

    static class Body implements Cloneable {
        public Head head;

        public Body() {
        }

        public Body(Head head) {
            this.head = head;
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            Body newBody = (Body) super.clone();
            newBody.head = (Head) head.clone();
            return newBody;
        }
    }

    static class Face implements Cloneable{
        private Integer faceId;

        public Face(Integer faceId) {
            this.faceId = faceId;
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            Face clone = (Face)super.clone();
            clone.faceId = new Integer(clone.faceId);
            return clone;
        }
    }

    static class Head implements Cloneable {
        public Face face;

        public Head(Face face) {
            this.face = face;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Head head = (Head) o;
            return Objects.equals(face, head.face);
        }

        @Override
        public int hashCode() {
            return Objects.hash(face);
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            Head newHead = (Head) super.clone();
            newHead.face = (Face) face.clone();
            return newHead;
        }
    }

    public static void main(String[] args) throws CloneNotSupportedException {
        Body body = new Body(new Head(new Face(255)));
        Body body1 = (Body) body.clone();
        System.out.println("body == body1 : " + (body == body1));
        System.out.println("body.head == body1.head : " + (body.head == body1.head));
        body.head.face.faceId = 1;
        System.out.println("body == body1 : " + (body == body1));
        System.out.println("body.head == body1.head : " + (body.head == body1.head));
        body.equals(body1);
    }


}
