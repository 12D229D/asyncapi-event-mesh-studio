/**
 * Schema Compatibility & Evolution Guard (Confluent Schema Registry Model)
 * Evaluates Backward, Forward and Full compatibility between JSON Schemas / Avro definitions
 */

const SchemaCompatibility = (() => {
  /**
   * Evaluates compatibility between oldSchema (v1) and newSchema (v2)
   * Modes: 'BACKWARD', 'FORWARD', 'FULL', 'NONE'
   */
  function evaluate(oldSchema, newSchema, mode = 'BACKWARD') {
    const issues = [];

    if (!oldSchema || !newSchema) {
      return {
        isCompatible: false,
        mode,
        summary: 'Ambos esquemas deben estar definidos para evaluar compatibilidad.',
        issues: [{ type: 'breaking', path: '$', message: 'Esquema v1 o v2 inválido o vacío.' }]
      };
    }

    const oldProps = oldSchema.properties || {};
    const newProps = newSchema.properties || {};
    const oldRequired = new Set(oldSchema.required || []);
    const newRequired = new Set(newSchema.required || []);

    // 1. Check Field Type Mutations (Breaking in both Backward & Forward)
    for (const [key, oldDef] of Object.entries(oldProps)) {
      if (newProps[key]) {
        const newDef = newProps[key];
        if (oldDef.type && newDef.type && oldDef.type !== newDef.type) {
          issues.push({
            type: 'breaking',
            path: `properties.${key}.type`,
            message: `Mutación de tipo incompatible: '${key}' cambió de '${oldDef.type}' a '${newDef.type}'.`,
            remedy: 'Mantener el tipo original o crear un nuevo canal/tópico con sufijo v2.'
          });
        }

        // Enum changes
        if (oldDef.enum && newDef.enum) {
          const removedEnumValues = oldDef.enum.filter(val => !newDef.enum.includes(val));
          if (removedEnumValues.length > 0) {
            issues.push({
              type: 'breaking',
              path: `properties.${key}.enum`,
              message: `Valores de enumeración eliminados en '${key}': [${removedEnumValues.join(', ')}].`,
              remedy: 'No eliminar valores de enumeración existentes en el Schema Registry.'
            });
          }
        }
      }
    }

    // 2. Mode-Specific Rules
    if (mode === 'BACKWARD' || mode === 'FULL') {
      // BACKWARD: New schema can read data written by old schema.
      // Rule: New required fields CANNOT be added unless they have a 'default' value.
      for (const reqField of newRequired) {
        if (!oldRequired.has(reqField)) {
          const fieldDef = newProps[reqField] || {};
          if (fieldDef.default === undefined) {
            issues.push({
              type: 'breaking',
              path: `required['${reqField}']`,
              message: `Campo obligatorio nuevo '${reqField}' agregado sin valor 'default'. Rompe consumidores actualizados leyendo mensajes antiguos.`,
              remedy: `Agrega un valor por defecto al campo '${reqField}': { default: ... } o hazlo opcional.`
            });
          }
        }
      }
    }

    if (mode === 'FORWARD' || mode === 'FULL') {
      // FORWARD: Old schema can read data written by new schema.
      // Rule: Old required fields CANNOT be deleted in new schema.
      for (const reqField of oldRequired) {
        if (!newProps[reqField]) {
          issues.push({
            type: 'breaking',
            path: `properties.${reqField}`,
            message: `El campo obligatorio antiguo '${reqField}' fue eliminado. Rompe consumidores legacy esperando este atributo.`,
            remedy: `Conservar el campo '${reqField}' o marcarlo como opcional con valor nulo.`
          });
        }
      }
    }

    // Check additions (Informational / Non-breaking)
    for (const key of Object.keys(newProps)) {
      if (!oldProps[key]) {
        issues.push({
          type: 'info',
          path: `properties.${key}`,
          message: `Nuevo campo opcional '${key}' incorporado al esquema v2.`
        });
      }
    }

    const breakingCount = issues.filter(i => i.type === 'breaking').length;
    const isCompatible = breakingCount === 0;

    let summary = '';
    if (isCompatible) {
      summary = `✅ El esquema v2 cumple con la compatibilidad ${mode}. Es seguro desplegarlo sin interrumpir a los consumidores en ejecución.`;
    } else {
      summary = `❌ Se detectaron ${breakingCount} cambio(s) disruptivo(s) bajo el modo ${mode}. Desplegar este esquema provocará fallos en producción.`;
    }

    return {
      isCompatible,
      mode,
      summary,
      breakingCount,
      issues
    };
  }

  return {
    evaluate
  };
})();

if (typeof window !== 'undefined') {
  window.SchemaCompatibility = SchemaCompatibility;
}
