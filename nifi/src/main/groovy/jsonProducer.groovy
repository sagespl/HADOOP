import groovy.json.JsonOutput
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets

/******************************************************************************
 * Variables provided in scope by script engine:
 *
 *     session - ProcessSession
 *     context - ProcessContext
 *     log - ComponentLog
 *     REL_SUCCESS - Relationship
 *     REL_FAILURE - Relationship
 */
ProcessSession session = session
Relationship success = REL_SUCCESS
Relationship failure = REL_FAILURE

FlowFile flowFile = session.get()

if (flowFile != null) {

    def path = flowFile.getAttribute('absolute.path')
    def filename = flowFile.getAttribute('filename')

    println "File: $path$filename "

    flowFile = session.putAttribute(flowFile, 'nowyparametr', "Ala ma Kota")

    def json = JsonOutput.toJson([name: 'Ala', age: 27, address: "Warszawa"])

    flowFile = session.write(flowFile, { outputStream ->
        outputStream.write(json.getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    session.transfer(flowFile, success)
}