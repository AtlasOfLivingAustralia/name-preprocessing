<?xml version="1.0"?>
<!--
  ~ Copyright (c) 2021-2022.  Atlas of Living Australia
  ~  All Rights Reserved.
  ~
  ~  The contents of this file are subject to the Mozilla Public
  ~  License Version 1.1 (the "License"); you may not use this file
  ~  except in compliance with the License. You may obtain a copy of
  ~  the License at http://www.mozilla.org/MPL/
  ~
  ~  Software distributed under the License is distributed on an "AS  IS" basis,
  ~  WITHOUT WARRANTY OF ANY KIND, either express or
  ~  implied. See the License for the specific language governing
  ~  rights and limitations under the License.
  -->

<xsl:stylesheet
 xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xmlns:vp="http://localhost/WebServiceVocabs/Schemas/Export"
 version="1.0"
 >
 <xsl:output method="text" encoding="UTF-8"/>

 <xsl:template match="/">
<xsl:apply-templates select="/vp:Vocabulary/vp:Subject"/>
 </xsl:template>

 <xsl:template match="vp:Subject">
  <xsl:variable name="englishPreferred" select="vp:Terms/vp:Preferred_Term[vp:Term_Languages/vp:Term_Language/vp:Language = '70051/English']"/>
  <xsl:variable name="englishNonPreferred" select="vp:Terms/vp:Non-Preferred_Term[vp:Term_Languages/vp:Term_Language/vp:Language = '70051/English']"/>
  <xsl:variable name="undeterminedNonPreferred" select="vp:Terms/vp:Non-Preferred_Term[vp:Term_Languages/vp:Term_Language/vp:Language = '70001/undetermined']"/>
  <xsl:variable name="englishNonPreferredCurrent" select="$englishNonPreferred[vp:Historic_Flag = 'Current']"/>
  <xsl:variable name="undeterminedNonPreferredCurrent" select="$undeterminedNonPreferred[vp:Historic_Flag = 'Current']"/>
  <xsl:variable name="englishName">
   <xsl:choose>
    <xsl:when test="$englishPreferred"><xsl:value-of select="$englishPreferred/vp:Term_Text"/></xsl:when>
    <xsl:when test="$englishNonPreferredCurrent"><xsl:value-of select="$englishNonPreferredCurrent[1]/vp:Term_Text"/></xsl:when>
    <xsl:otherwise><xsl:value-of select="vp:Terms/vp:Preferred_Term/vp:Term_Text"/></xsl:otherwise>
   </xsl:choose>
  </xsl:variable>
<xsl:value-of select="@Subject_ID"/>,<!--
--><xsl:value-of select="vp:Parent_Relationships/vp:Preferred_Parent/vp:Parent_Subject_ID"/>,<!--
-->"<xsl:value-of select="$englishName"/>",<!--
-->"<xsl:value-of select="vp:Terms/vp:Preferred_Term/vp:Term_Text"/>",<!--
-->"<xsl:for-each select="$englishNonPreferredCurrent | $undeterminedNonPreferredCurrent"><xsl:value-of select="vp:Term_Text"/><xsl:if test="position() != last()">|</xsl:if></xsl:for-each>",<!--
--><xsl:value-of select="vp:Terms/vp:Non-Preferred_Term[vp:Other_Flags = 'ISO 2-letter']/vp:Term_Text"/>,<!--
--><xsl:value-of select="vp:Terms/vp:Non-Preferred_Term[vp:Other_Flags = 'ISO 3-letter']/vp:Term_Text"/>,<!--
--><xsl:value-of select="vp:Place_Types/vp:Preferred_Place_Type/vp:Historic_Flag"/>,<!--
--><xsl:value-of select="vp:Place_Types/vp:Preferred_Place_Type/vp:Place_Type_ID"/>,<!--
--><xsl:value-of select="vp:Coordinates/vp:Standard/vp:Latitude/vp:Decimal"/>,<!--
--><xsl:value-of select="vp:Coordinates/vp:Standard/vp:Longitude/vp:Decimal"/><!--
--><xsl:text>
</xsl:text>
 </xsl:template>

</xsl:stylesheet>
