// convert a string into an array of string
package stringUDF;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class stringToArray extends EvalFunc<DataBag>
{	
	public DataBag exec(Tuple input) throws IOException {
		//if (input == null || input.size() == 0) return null;
		DataBag outBag = BagFactory.getInstance().newDefaultBag();

		try{
			if (input == null || input.size() == 0) {
				String myStr = null;
				Tuple myTuple = TupleFactory.getInstance().newTuple(myStr);
				outBag.add(myTuple);
				return outBag;
			}
			else {
				String myStr = (String) input.get(0);
				Tuple myTuple = TupleFactory.getInstance().newTuple(myStr);
				outBag.add(myTuple);
				return outBag;
			}
		}catch(Exception e){
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	public Schema outputSchema(Schema input) {
		try {
			Schema stringSchema = new Schema();
	        stringSchema.add(new Schema.FieldSchema("ARRAY_ELEM", DataType.CHARARRAY));
	        Schema bagSchema = new Schema(stringSchema);
	        Schema.FieldSchema bagFs = new Schema.FieldSchema("PIG_WRAPPER",bagSchema, DataType.BAG);
	        return new Schema(bagFs);        
		}catch(Exception e){
			return null;
		}
	}
		
}
