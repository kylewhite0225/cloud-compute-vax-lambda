using System.Xml.Serialization;

namespace ParseDB;

public class Vaccines
{
    public String brand { get; set; }

    public int total { get; set; }

    public int firstShot { get; set; }

    public int secondShot { get; set; }
}

public class XmlVaccines
{
    [XmlAttribute]
    public String name { get; set; }

    public int total { get; set; }

    public int firstShot { get; set; }

    public int secondShot { get; set; }
}