<?php

namespace Keboola\Datatype\Definition;

interface DefinitionInterface
{
    /**
     * @return string
     */
    public function getSQLDefinition();

    /**
     * @return array
     */
    public function toArray();

    /**
     * @return string
     */
    public function getBasetype();
}
