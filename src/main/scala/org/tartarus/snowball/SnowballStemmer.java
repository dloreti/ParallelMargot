
package org.tartarus.snowball;

import java.io.Serializable;

public abstract class SnowballStemmer extends SnowballProgram implements Serializable{
    public abstract boolean stem();
};
