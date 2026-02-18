namespace S3ServerLibrary
{
    using System.Xml;

    internal class XmlWriterExtended : XmlWriter
    {
        private XmlWriter baseWriter;

        /// <summary>
        /// </summary>
        public XmlWriterExtended(XmlWriter w)
        {
            baseWriter = w;
        }

        // Force WriteEndElement to use WriteFullEndElement
        /// <summary>
        /// </summary>
        public override void WriteEndElement() { baseWriter.WriteFullEndElement(); }

        /// <summary>
        /// </summary>
        public override void WriteFullEndElement()
        {
            baseWriter.WriteFullEndElement();
        }

        /// <summary>
        /// </summary>
        public override void Close()
        {
            baseWriter.Close();
        }

        /// <summary>
        /// </summary>
        public override void Flush()
        {
            baseWriter.Flush();
        }

        /// <summary>
        /// </summary>
        public override string LookupPrefix(string ns)
        {
            return (baseWriter.LookupPrefix(ns));
        }

        /// <summary>
        /// </summary>
        public override void WriteBase64(byte[] buffer, int index, int count)
        {
            baseWriter.WriteBase64(buffer, index, count);
        }

        /// <summary>
        /// </summary>
        public override void WriteCData(string text)
        {
            baseWriter.WriteCData(text);
        }

        /// <summary>
        /// </summary>
        public override void WriteCharEntity(char ch)
        {
            baseWriter.WriteCharEntity(ch);
        }

        /// <summary>
        /// </summary>
        public override void WriteChars(char[] buffer, int index, int count)
        {
            baseWriter.WriteChars(buffer, index, count);
        }

        /// <summary>
        /// </summary>
        public override void WriteComment(string text)
        {
            baseWriter.WriteComment(text);
        }

        /// <summary>
        /// </summary>
        public override void WriteDocType(string name, string pubid, string sysid, string subset)
        {
            baseWriter.WriteDocType(name, pubid, sysid, subset);
        }

        /// <summary>
        /// </summary>
        public override void WriteEndAttribute()
        {
            baseWriter.WriteEndAttribute();
        }

        /// <summary>
        /// </summary>
        public override void WriteEndDocument()
        {
            baseWriter.WriteEndDocument();
        }

        /// <summary>
        /// </summary>
        public override void WriteEntityRef(string name)
        {
            baseWriter.WriteEntityRef(name);
        }

        /// <summary>
        /// </summary>
        public override void WriteProcessingInstruction(string name, string text)
        {
            baseWriter.WriteProcessingInstruction(name, text);
        }

        /// <summary>
        /// </summary>
        public override void WriteRaw(string data)
        {
            baseWriter.WriteRaw(data);
        }

        /// <summary>
        /// </summary>
        public override void WriteRaw(char[] buffer, int index, int count)
        {
            baseWriter.WriteRaw(buffer, index, count);
        }

        /// <summary>
        /// </summary>
        public override void WriteStartAttribute(string prefix, string localName, string ns)
        {
            baseWriter.WriteStartAttribute(prefix, localName, ns);
        }

        /// <summary>
        /// </summary>
        public override void WriteStartDocument(bool standalone)
        {
            baseWriter.WriteStartDocument(standalone);
        }

        /// <summary>
        /// </summary>
        public override void WriteStartDocument()
        {
            baseWriter.WriteStartDocument();
        }

        /// <summary>
        /// </summary>
        public override void WriteStartElement(string prefix, string localName, string ns)
        {
            baseWriter.WriteStartElement(prefix, localName, ns);
        }

        /// <summary>
        /// </summary>
        public override WriteState WriteState
        {
            get { return baseWriter.WriteState; }
        }

        /// <summary>
        /// </summary>
        public override void WriteString(string text)
        {
            baseWriter.WriteString(text);
        }

        /// <summary>
        /// </summary>
        public override void WriteSurrogateCharEntity(char lowChar, char highChar)
        {
            baseWriter.WriteSurrogateCharEntity(lowChar, highChar);
        }

        /// <summary>
        /// </summary>
        public override void WriteWhitespace(string ws)
        {
            baseWriter.WriteWhitespace(ws);
        }
    }
}
