import StringIO
import traceback

__all__ = [
    "FieldError",
    "StreamError",
    "StreamRuntimeError"
]

class FieldError(Exception):
    """Exception raised on field incompatibility or missing fields."""
    pass
    
class StreamError(Exception):
    """Exception raised on stream."""
    pass

class StreamRuntimeError(Exception):
    """Exception raised when a node fails during `run()` phase.

    Attributes:
        * `message`: exception message
        * `node`: node where exception was raised
        * `exception`: exception that was raised while running the node
        * `traceback`: stack traceback
        * `inputs`: array of field lists for each input
        * `output`: output field list
    """
    def __init__(self, message=None, node=None, exception=None):
        super(StreamRuntimeError, self).__init__()
        if message:
            self.message = message
        else:
            self.message = ""

        self.node = node
        self.exception = exception
        self.traceback = None
        self.inputs = []
        self.output = []
        self.attributes = {}

    def print_exception(self, output=None):
        """Prints exception and details in human readable form. You can specify IO stream object in
        `output` parameter. By default text is printed to standard output."""

        if not output:
            output = sys.stdout

        text = u"stream failed. reason: %s\n" % self.message
        text += u"exception: %s: \n" % self.exception.__class__.__name__

        text += u"node: %s\n" % self.node

        try:
            text += unicode(self.exception)
        except Exception, e:
            text += u"<unable to get exception string: %s>" % e

        text += "\ntraceback\n"

        try:
            l = traceback.format_list(traceback.extract_tb(self.traceback))
            text += "".join(l)
        except Exception as e:
            text += "<unable to get traceback string: %s>" % e

        text += "\n"

        if self.inputs:
            for i, fields in enumerate(self.inputs):
                text += "input %i:\n" % i
                input_text = ""
                for (index, field) in enumerate(fields):
                    input_text += u"% 5d %s (storage:%s analytical:%s)\n" \
                                % (index, field.name, field.storage_type, field.analytical_type)
                text += unicode(input_text)
        else:
            text += "input: none"

        text += "\n"

        if self.output:
            text += "output:\n"
            for field in self.output:
                text += u"    %s (storage:%s analytical:%s)\n" \
                            % (field.name, field.storage_type, field.analytical_type)
        else:
            text += "ouput: none"

        text += "\n"

        if self.attributes:
            text += "attributes:\n"
            for name, attribute in self.attributes.items():
                try:
                    value = unicode(attribute)
                except Exception, e:
                    value = "unable to convert to string (exception: %s)" % e
                text += "    %s: %s\n" % (name, value)
        else:
            text += "attributes: none"

        output.write(text)

    def __str__(self):
        s = StringIO.StringIO()
        try:
            self.print_exception(s)
            v = s.getvalue()
        except Exception, e:
            v = "Unable to print strem exception. Reason: %s (%s)" % (e, type(e))
        finally:
            s.close()

        return v